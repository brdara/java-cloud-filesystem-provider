package com.uk.xarixa.cloud.filesystem.core.nio.file;

import java.io.IOException;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.nio.file.WatchKey;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.SchedulerException;
import org.quartz.SimpleTrigger;
import org.quartz.TriggerKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.uk.xarixa.cloud.filesystem.core.nio.CloudPath;
import com.uk.xarixa.cloud.filesystem.core.scheduling.SchedulingService;

public class PollingCloudWatchService implements CloudWatchService {
	public static final String JOB_GROUP_NAME = "PollingCloudWatchService";
	private static final Logger LOG = LoggerFactory.getLogger(PollingCloudWatchService.class);
	private final long pollTimeMs;
	private final List<TriggerKey> triggers = new ArrayList<>();
	private final List<PollingJobWatchKey> watchKeys = new ArrayList<>();
	private final LinkedBlockingDeque<PollingJobWatchKey> watchKeysReady = new LinkedBlockingDeque<>();

	protected PollingCloudWatchService(long pollTimeMs) {
		this.pollTimeMs = pollTimeMs;
	}

	@Override
	public synchronized void close() throws IOException {
		LOG.info("Closing down {}", this.getClass().getSimpleName());
		SchedulingService scheduler = SchedulingService.getInstance();
		
		for (TriggerKey key : triggers) {
			try {
				scheduler.unschedule(key);
			} catch (SchedulerException e) {
				LOG.error("Could not unschedule job with trigger {}", key, e);
			}
		}
		
		triggers.clear();
		watchKeys.clear();
		watchKeysReady.clear();
		LOG.info("Closed down {} OK", this.getClass().getSimpleName());
	}

	@Override
	public WatchKey poll() {
		return watchKeysReady.poll();
	}

	@Override
	public WatchKey poll(long timeout, TimeUnit unit) throws InterruptedException {
		return watchKeysReady.poll(timeout, unit);
	}

	@Override
	public WatchKey take() throws InterruptedException {
		return watchKeysReady.take();
	}

	@Override
	public WatchKey register(CloudPath path, Kind<?>[] events, Modifier... modifiers) {
		SchedulingService scheduler = SchedulingService.getInstance();
		PollingJobWatchKey watchKey = new PollingJobWatchKey();
		JobDataMap jobDataMap = new JobDataMap(
				Map.of(PollingJob.JOB_PATH_KEY, path,
						PollingJob.JOB_KIND_KEY, Arrays.asList(events),
						PollingJob.JOB_MODIFIERS_KEY, Arrays.asList(modifiers),
						PollingJob.JOB_WATCH_KEY, watchKey));
		String jobId = path.toString();
		JobDetail jobDetail = scheduler.createJob(jobId, JOB_GROUP_NAME, PollingJob.class, jobDataMap);
		SimpleTrigger trigger = scheduler.createIntervalTrigger(jobId, JOB_GROUP_NAME, pollTimeMs);

		TriggerKey triggerKey;
		try {
			triggerKey = scheduler.schedule(jobDetail, trigger);
		} catch (SchedulerException e) {
			LOG.error("Could not schedule watch service job", e);
			throw new RuntimeException("Could not schedule watch service job");
		}

		// Add the watch key to the list
		watchKeys.add(watchKey);
		triggers.add(triggerKey);

		return watchKey;
	}

}
