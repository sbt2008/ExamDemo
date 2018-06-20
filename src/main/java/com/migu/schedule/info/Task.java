package com.migu.schedule.info;

import java.util.ArrayList;
import java.util.Collections;

/**
 * 
 * @author sbt2008
 *
 */
public class Task extends TaskInfo implements Comparable<Task> {

	/**
	 * 消耗率
	 */
	private int consumption;

	public Task(int taskId, int consumption) {
		setTaskId(taskId);
		setNodeId(-1);// 初始为-1

		this.consumption = consumption;
	}

	public int getConsumption() {
		return consumption;
	}

	public void setConsumption(int consumption) {
		this.consumption = consumption;
	}

	public int compareTo(Task task) {
		return this.getTaskId() - task.getTaskId();
	}

	@Override
	public String toString() {
		return super.toString();
	}

	public static void main(String[] args) {
		// for test order
		ArrayList<Task> taskList = new ArrayList<Task>();

		taskList.add(new Task(1, 10));
		taskList.add(new Task(2, 10));

		Collections.sort(taskList);

		System.out.println(taskList);
	}
}
