package com.travelex.cs.reports.aws.dms;

import java.io.Serializable;
import java.time.LocalDateTime;

import com.amazonaws.util.StringUtils;

public class Notification implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static final CharSequence FULL_LOAD_ONLY_FINISHED = "FULL_LOAD_ONLY_FINISHED";

	private static final String MIBI_LOAD_COMPLETED = "Yet to Start";

	private String migrationEndpoint;
	private String timeOfExecution;
	private String replicationTaskStartDate;
	private String replicationTaskStartedToday;
	private String executionStatus;
	private String stopReason;
	private String replicationTaskStats;
	private String notificationsql;
	private String result;

	public Notification(String asText) {

	}

	public Notification(String migrationEndpoint, String timeOfExecution,
			String replicationTaskStartDate,
			String replicationTaskStartedToday, String executionStatus,
			String stopReason, String replicationTaskStats,
			String notificationsql, String result) {
		super();
		this.migrationEndpoint = migrationEndpoint;
		this.timeOfExecution = timeOfExecution;
		this.replicationTaskStartDate = replicationTaskStartDate;
		this.replicationTaskStartedToday = replicationTaskStartedToday;
		this.executionStatus = executionStatus;
		this.stopReason = stopReason;
		this.replicationTaskStats = replicationTaskStats;
		this.notificationsql = notificationsql;
		this.result = result;
	}

	public String getMigrationEndpoint() {
		return migrationEndpoint;
	}

	public String getTimeOfExecution() {
		return timeOfExecution;
	}

	public String getReplicationTaskStartDate() {
		return replicationTaskStartDate;
	}

	public String getReplicationTaskStartedToday() {
		return replicationTaskStartedToday;
	}

	public String getExecutionStatus() {
		return executionStatus;
	}

	public String getStopReason() {
		return stopReason;
	}

	public String getReplicationTaskStats() {
		return replicationTaskStats;
	}

	public String getNotificationsql() {
		return notificationsql == null ? notificationsql = toSql()
				: notificationsql;
	}

	public String getResult() {
		return result;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((executionStatus == null) ? 0 : executionStatus.hashCode());
		result = prime
				* result
				+ ((migrationEndpoint == null) ? 0 : migrationEndpoint
						.hashCode());
		result = prime * result
				+ ((notificationsql == null) ? 0 : notificationsql.hashCode());
		result = prime
				* result
				+ ((replicationTaskStartDate == null) ? 0
						: replicationTaskStartDate.hashCode());
		result = prime
				* result
				+ ((replicationTaskStartedToday == null) ? 0
						: replicationTaskStartedToday.hashCode());
		result = prime
				* result
				+ ((replicationTaskStats == null) ? 0 : replicationTaskStats
						.hashCode());
		result = prime * result
				+ ((this.result == null) ? 0 : this.result.hashCode());
		result = prime * result
				+ ((stopReason == null) ? 0 : stopReason.hashCode());
		result = prime * result
				+ ((timeOfExecution == null) ? 0 : timeOfExecution.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Notification other = (Notification) obj;
		if (executionStatus == null) {
			if (other.executionStatus != null)
				return false;
		} else if (!executionStatus.equals(other.executionStatus))
			return false;
		if (migrationEndpoint == null) {
			if (other.migrationEndpoint != null)
				return false;
		} else if (!migrationEndpoint.equals(other.migrationEndpoint))
			return false;
		if (notificationsql == null) {
			if (other.notificationsql != null)
				return false;
		} else if (!notificationsql.equals(other.notificationsql))
			return false;
		if (replicationTaskStartDate == null) {
			if (other.replicationTaskStartDate != null)
				return false;
		} else if (!replicationTaskStartDate
				.equals(other.replicationTaskStartDate))
			return false;
		if (replicationTaskStartedToday == null) {
			if (other.replicationTaskStartedToday != null)
				return false;
		} else if (!replicationTaskStartedToday
				.equals(other.replicationTaskStartedToday))
			return false;
		if (replicationTaskStats == null) {
			if (other.replicationTaskStats != null)
				return false;
		} else if (!replicationTaskStats.equals(other.replicationTaskStats))
			return false;
		if (result == null) {
			if (other.result != null)
				return false;
		} else if (!result.equals(other.result))
			return false;
		if (stopReason == null) {
			if (other.stopReason != null)
				return false;
		} else if (!stopReason.equals(other.stopReason))
			return false;
		if (timeOfExecution == null) {
			if (other.timeOfExecution != null)
				return false;
		} else if (!timeOfExecution.equals(other.timeOfExecution))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "TaskDto [migrationEndpoint=" + migrationEndpoint
				+ ", timeOfExecution=" + timeOfExecution
				+ ", replicationTaskStartDate=" + replicationTaskStartDate
				+ ", replicationTaskStartedToday="
				+ replicationTaskStartedToday + ", executionStatus="
				+ executionStatus + ", stopReason=" + stopReason
				+ ", replicationTaskStats=" + replicationTaskStats
				+ ", notificationsql=" + notificationsql + ", result=" + result
				+ "]";
	}

	protected String toSql() {
		StringBuilder build = new StringBuilder();
		build.append("insert into dms_task_log values('")
				.append(getTimeOfExecution()).append("','")
				.append(migrationEndpoint).append("','")
				.append(replicationTaskStartDate).append("','")
				.append(replicationTaskStats).append("','")
				.append(getTaskResult()).append("','")
				.append(getJobResultStatus()).append("','")
				.append(MIBI_LOAD_COMPLETED).append("'")
				.append(LocalDateTime.now()).append("'");
		return build.toString();
	}

	public String getJobResultStatus() {
		return (!StringUtils.isNullOrEmpty(stopReason))
				&& stopReason.contains(FULL_LOAD_ONLY_FINISHED) ? "Y"
				: "N";
	}

	public String getTaskResult() {
		return !StringUtils.isNullOrEmpty(stopReason)?stopReason:"";
	}

	public static class Builder {
		private String migrationEndpoint;
		private String timeOfExecution;
		private String replicationTaskStartDate;
		private String replicationTaskStartedToday;
		private String executionStatus;
		private String stopReason;
		private String replicationTaskStats;
		private String notificationsql;
		private String result;

		public Builder migrationEndPoint(final String migrationEndpoint) {
			this.migrationEndpoint = migrationEndpoint;
			return this;
		}

		public Builder timeOfExecution(final String timeOfExecution) {
			this.timeOfExecution = timeOfExecution;
			return this;
		}

		public Builder replicationTaskStartDate(
				final String replicationTaskStartDate) {
			this.replicationTaskStartDate = replicationTaskStartDate;
			return this;
		}

		public Builder replicationTaskStartedToday(
				final String replicationTaskStartedToday) {
			this.replicationTaskStartedToday = replicationTaskStartedToday;
			return this;
		}

		public Builder executionStatus(final String executionStatus) {
			this.executionStatus = executionStatus;
			return this;
		}

		public Builder stopReason(final String stopReason) {
			this.stopReason = stopReason;
			return this;
		}

		public Builder replicationTaskStats(final String replicationTaskStats) {
			this.replicationTaskStats = replicationTaskStats;
			return this;
		}

		public Builder notificationsql(final String notificationsql) {
			this.notificationsql = notificationsql;
			return this;
		}

		public Builder result(final String result) {
			this.result = result;
			return this;
		}

		public Notification build() {
			return new Notification(migrationEndpoint, timeOfExecution,
					replicationTaskStartDate, replicationTaskStartedToday,
					executionStatus, stopReason, replicationTaskStats,
					notificationsql, result);
		}
	}
}
