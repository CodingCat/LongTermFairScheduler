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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.metrics.MetricsContext;

/**
 * A schedulable pool of jobs.
 */
public class Pool {
  /** Name of the default pool, where jobs with no pool parameter go. */
  public static final String DEFAULT_POOL_NAME = "default";
  
  /** Pool name. */
  private String name;
  
  /** Jobs in this specific pool; does not include children pools' jobs. */
  private Collection<JobInProgress> jobs = new ArrayList<JobInProgress>();
  
  /** Scheduling mode for jobs inside the pool (fair or FIFO) */
  private SchedulingMode schedulingMode;

  private PoolSchedulable mapSchedulable;
  private PoolSchedulable reduceSchedulable;
  
  
  private float mapCredit = 0;
  private float reduceCredit = 0;
  
  private int nFinishedjobs = 0;
  private float responseTime = 0;
  private float stretch = 0;
  
  public Pool(FairScheduler scheduler, String name) {
    this.name = name;
    mapSchedulable = new PoolSchedulable(scheduler, this, TaskType.MAP);
    reduceSchedulable = new PoolSchedulable(scheduler, this, TaskType.REDUCE);
  }
  
  public Collection<JobInProgress> getJobs() {
    return jobs;
  }
  
  public void addJob(JobInProgress job) {
    jobs.add(job);
    mapSchedulable.addJob(job);
    reduceSchedulable.addJob(job);
  }
  
  public void removeJob(JobInProgress job) {
	float existingJobResponseTime = (job.finishTime - job.startTime)/1000;
    jobs.remove(job);
    mapSchedulable.removeJob(job);
    reduceSchedulable.removeJob(job);
    //update metrics
    nFinishedjobs++;
    
    responseTime = (responseTime * (nFinishedjobs - 1) + existingJobResponseTime) 
    		/ nFinishedjobs;
    stretch = (stretch * (nFinishedjobs - 1) + existingJobResponseTime / ((float)job.getInputLength()/1024)) 
    		/ nFinishedjobs;
  }
  
  public String getName() {
    return name;
  }
  
  public float getResponseTime(){
	  return responseTime;
  }
  
  public float getStretch(){
	  return stretch;
  }

  public SchedulingMode getSchedulingMode() {
    return schedulingMode;
  }
  
  public void setSchedulingMode(SchedulingMode schedulingMode) {
    this.schedulingMode = schedulingMode;
  }

  public boolean isDefaultPool() {
    return Pool.DEFAULT_POOL_NAME.equals(name);
  }
  
  public PoolSchedulable getMapSchedulable() {
    return mapSchedulable;
  }
  
  
  public PoolSchedulable getReduceSchedulable() {
    return reduceSchedulable;
  }
  
  public PoolSchedulable getSchedulable(TaskType type) {
    return type == TaskType.MAP ? mapSchedulable : reduceSchedulable;
  }

  public void updateMetrics() {
    mapSchedulable.updateMetrics();
    reduceSchedulable.updateMetrics();
  }

  public int getRunningTasks(TaskType ttype){
	PoolSchedulable taskSchedulable = (ttype == TaskType.MAP ? mapSchedulable : reduceSchedulable);
	return taskSchedulable.getRunningTasks();
  }
  
  public int getDemand(TaskType ttype){
	PoolSchedulable taskSchedulable = (ttype == TaskType.MAP ? mapSchedulable : reduceSchedulable);
	return taskSchedulable.getDemand();
  }
  
  public void updateCredit(TaskType ttype, float l){
	  if (ttype == TaskType.MAP){
		  this.mapCredit += l;
	  }
	  if (ttype == TaskType.REDUCE){
		  this.reduceCredit += l;
	  }
  }
  
  public float getCredit(TaskType ttype){
	  return ((ttype == TaskType.MAP ? this.mapCredit : this.reduceCredit));
  }
}
