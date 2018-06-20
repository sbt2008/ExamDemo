package com.migu.schedule.info;

import java.util.ArrayList;
import java.util.List;

import com.migu.schedule.service.TaskService;

/**
 * @author sbt2008
 *
 */
public class ServerNode implements Comparable<ServerNode> {
	/**
	 * 节点Id
	 */
	private int nodeId;

	private List<Task> taskList = new ArrayList<Task>();

	public int getNodeId() {
		return nodeId;
	}

	public void setNodeId(int nodeId) {
		this.nodeId = nodeId;
	}

	/**
	 * 获得总功率
	 * 
	 * @return
	 */
	public synchronized int getTotalConsumption() {
		int total = 0;
		for (Task task : taskList) {
			total += task.getConsumption();
		}
		return total;
	}

	/**
	 * 添加任务
	 * 
	 * @param task
	 */
	public synchronized void addTask(Task task) {
		task.setNodeId(nodeId);
		taskList.add(task);
		TaskService instance = TaskService.getInstance();
		instance.getHangupTaskList().remove(task);
		instance.getRunTaskList().add(task);
	}

	public int compareTo(ServerNode node) {
		return this.getNodeId() - node.getNodeId();
	}

	@Override
	public String toString() {
		return "ServerNode [nodeId=" + nodeId + " total=" + getTotalConsumption() + "]";
	}
	
	public void clear(){
		taskList.clear();
	}

}
