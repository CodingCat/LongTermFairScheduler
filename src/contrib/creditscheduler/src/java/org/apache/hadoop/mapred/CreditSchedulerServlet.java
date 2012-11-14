/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.mapred.CreditScheduler.JobInfo;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.StringUtils;

/**
 * Servlet for displaying fair scheduler information, installed at
 * [job tracker URL]/scheduler when the {@link FairScheduler} is in use.
 * 
 * The main features are viewing each job's task count and fair share,
 * and admin controls to change job priorities and pools from the UI.
 * 
 * There is also an "advanced" view for debugging that can be turned on by
 * going to [job tracker URL]/scheduler?advanced.
 */
public class CreditSchedulerServlet extends HttpServlet {
  private static final long serialVersionUID = 9104070533067306659L;
  private static final DateFormat DATE_FORMAT = 
    new SimpleDateFormat("MMM dd, HH:mm");
  
  private CreditScheduler scheduler;
  private JobTracker jobTracker;
  private static long lastId = 0; // Used to generate unique element IDs

  @Override
  public void init() throws ServletException {
    super.init();
    ServletContext servletContext = this.getServletContext();
    this.scheduler = (CreditScheduler) servletContext.getAttribute("scheduler");
    this.jobTracker = (JobTracker) scheduler.taskTrackerManager;
  }
  
  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    doGet(req, resp); // Same handler for both GET and POST
  }
  
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws ServletException, IOException {
    // If the request has a set* param, handle that and redirect to the regular
    // view page so that the user won't resubmit the data if they hit refresh.
    boolean advancedView = request.getParameter("advanced") != null;
    if (JSPUtil.privateActionsAllowed(jobTracker.conf)
        && request.getParameter("setPool") != null) {
      Collection<JobInProgress> runningJobs = getInitedJobs();
      PoolManager poolMgr = null;
      synchronized (scheduler) {
        poolMgr = scheduler.getPoolManager();
      }
      String pool = request.getParameter("setPool");
      String jobId = request.getParameter("jobid");
      for (JobInProgress job: runningJobs) {
        if (job.getProfile().getJobID().toString().equals(jobId)) {
          synchronized(scheduler){
            poolMgr.setPool(job, pool);
          }
          scheduler.update();
          break;
        }
      }      
      response.sendRedirect("/scheduler" + (advancedView ? "?advanced" : ""));
      return;
    }
    if (JSPUtil.privateActionsAllowed(jobTracker.conf)
        && request.getParameter("setPriority") != null) {
      Collection<JobInProgress> runningJobs = getInitedJobs();
      JobPriority priority = JobPriority.valueOf(request.getParameter(
          "setPriority"));
      String jobId = request.getParameter("jobid");
      for (JobInProgress job: runningJobs) {
        if (job.getProfile().getJobID().toString().equals(jobId)) {
          job.setPriority(priority);
          scheduler.update();
          break;
        }
      }      
      response.sendRedirect("/scheduler" + (advancedView ? "?advanced" : ""));
      return;
    }
    // Print out the normal response
    response.setContentType("text/html");

    // Because the client may read arbitrarily slow, and we hold locks while
    // the servlet outputs, we want to write to our own buffer which we know
    // won't block.
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintWriter out = new PrintWriter(baos);
    String hostname = StringUtils.simpleHostname(
        jobTracker.getJobTrackerMachine());
    out.print("<html><head>");
    out.printf("<title>%s Credit Scheduler Administration</title>\n", hostname);
    out.print("<link rel=\"stylesheet\" type=\"text/css\" " + 
        "href=\"/static/hadoop.css\">\n");
    out.print("</head><body>\n");
    out.printf("<h1><a href=\"/jobtracker.jsp\">%s</a> " + 
        "Credit Scheduler Administration</h1>\n", hostname);
    showPools(out, advancedView);
    showJobs(out, advancedView);
    out.print("</body></html>\n");
    out.close();

    // Flush our buffer to the real servlet output
    OutputStream servletOut = response.getOutputStream();
    baos.writeTo(servletOut);
    servletOut.close();
  }

  /**
   * Print a view of pools to the given output writer.
   */
  private void showPools(PrintWriter out, boolean advancedView) {
    synchronized(scheduler) {
      boolean warnInverted = false;
      PoolManager poolManager = scheduler.getPoolManager();
      out.print("<h2>Pools</h2>\n");
      out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n");
      out.print("<tr><th rowspan=2>Pool</th>" +
          "<th rowspan=2>Running Jobs</th>" +
          "<th rowspan=2>Avr. ResponseTime (Sec.)</th>" +
          "<th rowspan=2>Avr. Slowdown</th>" +
          "<th rowspan=2>Input Size (MB)</th>" +
          "<th rowspan=2>Map Input Size (MB)</th>" +
          "<th rowspan=2>Reduce Input Size (MB)</th>" +
          "<th colspan=4>Map Tasks</th>" + 
          "<th colspan=4>Reduce Tasks</th></tr>\n<tr>" +
          "<th>Avr. Slots.Sec</th><th>Avr. Slowdown</th><th>Fair Share</th><th>Credits</th>" + 
          "<th>Avr. Slots.Sec</th><th>Avr. Slowdown</th><th>Fair Share</th><th>Credits</th></tr>\n");
      List<Pool> pools = new ArrayList<Pool>(poolManager.getPools());
      Collections.sort(pools, new Comparator<Pool>() {
        public int compare(Pool p1, Pool p2) {
          if (p1.isDefaultPool())
            return 1;
          else if (p2.isDefaultPool())
            return -1;
          else return p1.getName().compareTo(p2.getName());
        }});
      for (Pool pool: pools) {
        String name = pool.getName();
        boolean invertedMaps = poolManager.invertedMinMax(TaskType.MAP, name);
        boolean invertedReduces = poolManager.invertedMinMax(TaskType.REDUCE, name);
        warnInverted = warnInverted || invertedMaps || invertedReduces;
        out.print("<tr>");
        out.printf("<td>%s</td>", name);
        out.printf("<td>%d</td>", pool.getJobs().size());
        out.printf("<td>%f</td>", pool.getResponseTime());
        out.printf("<td>%f</td>", pool.getStretch());
        out.printf("<td>%f</td>", pool.getInputSize());
        out.printf("<td>%f</td>", pool.getMapInSize());
        out.printf("<td>%f</td>", pool.getReduceInSize());
        // Map Tasks
        out.printf("<td>%f</td>", pool.getMapResponseTime());
        out.printf("<td>%f</td>", pool.getMapStretch());
        out.printf("<td>%.1f</td>", pool.getMapSchedulable().getFairShare());
        out.printf("<td>%.1f</td>", pool.getCredit(TaskType.MAP));
        // Reduce Tasks
        out.printf("<td>%f</td>", pool.getReduceResponseTime());
        out.printf("<td>%f</td>", pool.getReduceStretch());
        out.printf("<td>%.1f</td>", pool.getReduceSchedulable().getFairShare());
        out.printf("<td>%.1f</td>", pool.getCredit(TaskType.REDUCE));
        out.print("</tr>\n");
      }
      out.print("</table>\n");
      if(warnInverted) {
        out.print("<p>* One or more pools have max share set lower than min share. Max share will be used and minimum will be treated as if set equal to max.</p>");
      }
    }
  }

  /**
   * Print a view of running jobs to the given output writer.
   */
  private void showJobs(PrintWriter out, boolean advancedView) {
    out.print("<h2>Running Jobs</h2>\n");
    out.print("<table border=\"2\" cellpadding=\"5\" cellspacing=\"2\">\n");
    int colsPerTaskType = advancedView ? 4 : 3;
    out.printf("<tr><th rowspan=2>Submitted</th>" + 
        "<th rowspan=2>JobID</th>" +
        "<th rowspan=2>User</th>" +
        "<th rowspan=2>Name</th>" +
        "<th rowspan=2>Pool</th>" +
        "<th rowspan=2>Priority</th>" +
        "<th colspan=%d>Map Tasks</th>" +
        "<th colspan=%d>Reduce Tasks</th>",
        colsPerTaskType, colsPerTaskType);
    out.print("</tr><tr>\n");
    out.print("<th>Finished</th><th>Running</th><th>Fair Share</th>" +
        (advancedView ? "<th>Weight</th>" : ""));
    out.print("<th>Finished</th><th>Running</th><th>Fair Share</th>" +
        (advancedView ? "<th>Weight</th>" : ""));
    out.print("</tr>\n");
    synchronized (jobTracker) {
      Collection<JobInProgress> runningJobs = getInitedJobs();
      synchronized (scheduler) {
        for (JobInProgress job: runningJobs) {
          JobProfile profile = job.getProfile();
          JobInfo info = scheduler.infos.get(job);
          if (info == null) { // Job finished, but let's show 0's for info
            info = new JobInfo(null, null);
          }
          out.print("<tr>\n");
          out.printf("<td>%s</td>\n", DATE_FORMAT.format(
              new Date(job.getStartTime())));
          out.printf("<td><a href=\"jobdetails.jsp?jobid=%s\">%s</a></td>",
              profile.getJobID(), profile.getJobID());
          out.printf("<td>%s</td>\n", profile.getUser());
          out.printf("<td>%s</td>\n", profile.getJobName());
          if (JSPUtil.privateActionsAllowed(jobTracker.conf)) {
            out.printf("<td>%s</td>\n", generateSelect(scheduler
                .getPoolManager().getPoolNames(), scheduler.getPoolManager()
                .getPoolName(job), "/scheduler?setPool=<CHOICE>&jobid="
                + profile.getJobID() + (advancedView ? "&advanced" : "")));
            out.printf("<td>%s</td>\n", generateSelect(Arrays
                .asList(new String[] { "VERY_LOW", "LOW", "NORMAL", "HIGH",
                    "VERY_HIGH" }), job.getPriority().toString(),
                "/scheduler?setPriority=<CHOICE>&jobid=" + profile.getJobID()
                    + (advancedView ? "&advanced" : "")));
          } else {
            out.printf("<td>%s</td>\n", scheduler.getPoolManager().getPoolName(job));
            out.printf("<td>%s</td>\n", job.getPriority().toString());
          }
          Pool pool = scheduler.getPoolManager().getPool(job);
          String mapShare = (pool.getSchedulingMode() == SchedulingMode.FAIR) ?
              String.format("%.1f", info.mapSchedulable.getFairShare()) : "NA";
          out.printf("<td>%d / %d</td><td>%d</td><td>%s</td>\n",
              job.finishedMaps(), job.desiredMaps(), 
              info.mapSchedulable.getRunningTasks(),
              mapShare);
          if (advancedView) {
            out.printf("<td>%.1f</td>\n", info.mapSchedulable.getWeight());
          }
          String reduceShare = (pool.getSchedulingMode() == SchedulingMode.FAIR) ?
              String.format("%.1f", info.reduceSchedulable.getFairShare()) : "NA";
          out.printf("<td>%d / %d</td><td>%d</td><td>%s</td>\n",
              job.finishedReduces(), job.desiredReduces(), 
              info.reduceSchedulable.getRunningTasks(),
              reduceShare);
          if (advancedView) {
            out.printf("<td>%.1f</td>\n", info.reduceSchedulable.getWeight());
          }
          out.print("</tr>\n");
        }
      }
    }
    out.print("</table>\n");
  }

  /**
   * Generate a HTML select control with a given list of choices and a given
   * option selected. When the selection is changed, take the user to the
   * <code>submitUrl</code>. The <code>submitUrl</code> can be made to include
   * the option selected -- the first occurrence of the substring
   * <code>&lt;CHOICE&gt;</code> will be replaced by the option chosen.
   */
  private String generateSelect(Iterable<String> choices, 
      String selectedChoice, String submitUrl) {
    StringBuilder html = new StringBuilder();
    String id = "select" + lastId++;
    html.append("<select id=\"" + id + "\" name=\"" + id + "\" " + 
        "onchange=\"window.location = '" + submitUrl + 
        "'.replace('<CHOICE>', document.getElementById('" + id +
        "').value);\">\n");
    for (String choice: choices) {
      html.append(String.format("<option value=\"%s\"%s>%s</option>\n",
          choice, (choice.equals(selectedChoice) ? " selected" : ""), choice));
    }
    html.append("</select>\n");
    return html.toString();
  }

  /**
   * Obtained all initialized jobs
   */
  private Collection<JobInProgress> getInitedJobs() {
    Collection<JobInProgress> runningJobs = jobTracker.getRunningJobs();
    for (Iterator<JobInProgress> it = runningJobs.iterator(); it.hasNext();) {
      JobInProgress job = it.next();
      if (!job.inited()) {
        it.remove();
      }
    }
    return runningJobs;
  }

}
