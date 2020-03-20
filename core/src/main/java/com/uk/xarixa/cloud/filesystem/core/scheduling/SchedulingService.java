package com.uk.xarixa.cloud.filesystem.core.scheduling;

import java.util.concurrent.atomic.AtomicBoolean;

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.DateBuilder;
import org.quartz.DateBuilder.IntervalUnit;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service to help with scheduling using Quartz
 * 
 * @author brdar
 */
public class SchedulingService {
	private final static Logger LOG = LoggerFactory.getLogger(SchedulingService.class);
	private static AtomicBoolean exists = new AtomicBoolean(false);
	private static SchedulingService service;

	private final Scheduler scheduler;

	public static SchedulingService getInstance() {
		if (exists.compareAndSet(false, true)) {
			try {
				service = new SchedulingService();
			} catch (Exception e) {
				exists.set(false);
				LOG.error("Could not create Quartz scheduler", e);
			}
		}
		
		if (!exists.get()) {
			throw new RuntimeException("Unable to get scheduling service, check the logs for errors");
		}
		
		return service;
	}

	private SchedulingService() throws SchedulerException {
		scheduler = StdSchedulerFactory.getDefaultScheduler();
		scheduler.start();
	}

	@Override
	protected void finalize() throws Throwable {
		scheduler.shutdown();
	}

	public void shutdown() throws Throwable {
		finalize();
	}

	public JobDetail createJob(String jobName, String groupName, Class<? extends Job> jobClass, JobDataMap jobDataMap) {
		return JobBuilder.newJob(jobClass)
			    .withIdentity(jobName, groupName)
			    .usingJobData(jobDataMap)
			    .requestRecovery()
			    .build();
	}
	
	public SimpleTrigger createIntervalTrigger(String triggerName, String triggerGroupName, long pollInterval, int startInFutureMs) {
		return TriggerBuilder.newTrigger()
				.withIdentity(TriggerKey.triggerKey(triggerName, triggerGroupName))
	            .withSchedule(SimpleScheduleBuilder.simpleSchedule()
	                     .withIntervalInMilliseconds(pollInterval)
	                     .repeatForever())
				.startAt(DateBuilder.futureDate(startInFutureMs, IntervalUnit.MILLISECOND))
				.build();
	}

	public SimpleTrigger createIntervalTrigger(String triggerName, String triggerGroupName, long pollInterval) {
		return TriggerBuilder.newTrigger()
				.withIdentity(TriggerKey.triggerKey(triggerName, triggerGroupName))
	            .withSchedule(SimpleScheduleBuilder.simpleSchedule()
	                     .withIntervalInMilliseconds(pollInterval)
	                     .repeatForever())
				.startNow()
				.build();
	}

	public CronTrigger createCronTrigger(String triggerName, String triggerGroupName, String cronExpression, int startInFutureMs) {
		return TriggerBuilder.newTrigger()
				.withIdentity(TriggerKey.triggerKey(triggerName, triggerGroupName))
	            .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
				.startAt(DateBuilder.futureDate(startInFutureMs, IntervalUnit.MILLISECOND))
				.build();
	}

	public CronTrigger createCronTrigger(String triggerName, String triggerGroupName, String cronExpression) {
		return TriggerBuilder.newTrigger()
				.withIdentity(TriggerKey.triggerKey(triggerName, triggerGroupName))
	            .withSchedule(CronScheduleBuilder.cronSchedule(cronExpression))
				.startNow()
				.build();
	}

	public TriggerKey schedule(JobDetail job, Trigger trigger) throws SchedulerException {
		scheduler.scheduleJob(job, trigger);
		return trigger.getKey();
	}
	
	public void unschedule(TriggerKey triggerKey) throws SchedulerException {
		scheduler.unscheduleJob(triggerKey);
	}

}
