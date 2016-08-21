package com.novemberain.quartz.mongodb;

import com.novemberain.quartz.mongodb.util.SerialUtils;
import org.bson.Document;
import org.quartz.JobDataMap;
import org.quartz.JobPersistenceException;

import java.io.IOException;
import java.util.Map;

/**
 * Converter between {@link JobDataMap} and mongo {@link Document}.
 */
public class JobDataConverter {

	private final boolean base64Preferred;

	/**
	 * Constructs an instance of converter.
	 * @param base64Preferred if preferred way to store job details is {@code base64}.
	 */
	public JobDataConverter(final boolean base64Preferred) {
		this.base64Preferred = base64Preferred;
	}

	/**
	 * Converts from job data map to document.
	 * Depending on config, job data map can be stored
	 * as a {@code base64} encoded or plain object.
	 * @param from {@link JobDataMap} to convert from.
	 * @param to mongo {@link Document} to populate. 
	 * @throws JobPersistenceException if could not encode.
	 */
	public void toDocument(JobDataMap from, Document to) throws JobPersistenceException {
		if (from.isEmpty()) {
			return;
		}
		if (base64Preferred) {
			String jobDataString;
			try {
				jobDataString = SerialUtils.serialize(from);
			} catch (IOException e) {
				throw new JobPersistenceException("Could not serialise job data.", e);
			}
			to.put(Constants.JOB_DATA, jobDataString);
		} else {
			to.put(Constants.JOB_DATA_PLAIN, from.getWrappedMap());
		}
	}

	/**
	 * Converts from document to job data map.
	 * If {@code base64} is preferred, tries
	 * to decode from '{@value Constants#JOB_DATA}' field.
	 * Otherwise, first reads a plain object from 
	 * '{@value Constants#JOB_DATA_PLAIN}' field, or,
	 * if not present, falls back to {@code base64} field.
	 * @param from mongo {@link Document} to read from.
	 * @param to {@link JobDataMap} to populate.
	 * @return if {@link JobDataMap} has been populated.
	 * @throws JobPersistenceException if could not decode.
	 */
	public boolean toJobData(Document from, JobDataMap to) throws JobPersistenceException {
		if (base64Preferred) {
			return toJobDataFromBase64(from, to);
		} else {
			if (toJobDataFromField(from, to)) {
				return true;
			}
			return toJobDataFromBase64(from, to);
		}
	}

	/**
	 * Converts from document to job data map
	 * reading {@code base64} encoded field
	 * '{@value Constants#JOB_DATA}'.
	 */
	private boolean toJobDataFromBase64(Document from, JobDataMap to) throws JobPersistenceException {
		String jobDataBase64String = from.getString(Constants.JOB_DATA);
		if (jobDataBase64String == null) {
			return false;
		}
		Map<String, ?> jobDataMap;
		try {
			jobDataMap = SerialUtils.deserialize(null, jobDataBase64String);
		} catch (IOException e) {
			throw new JobPersistenceException("Could not deserialize job data.", e);
		}
		to.putAll(jobDataMap);
		return true;
	}

	/**
	 * Converts from document to job data map
	 * reading a plain object from field
	 * '{@value Constants#JOB_DATA_PLAIN}'.
	 */
	private boolean toJobDataFromField(Document from, JobDataMap to) {
		@SuppressWarnings("unchecked")
		Map<String, ?> jobDataMap = from.get(Constants.JOB_DATA_PLAIN, Map.class);
		if (jobDataMap == null) {
			return false;
		}
		to.putAll(jobDataMap);
		return true;
	}
}
