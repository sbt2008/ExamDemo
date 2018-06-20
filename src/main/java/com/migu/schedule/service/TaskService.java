package com.migu.schedule.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.ServerNode;
import com.migu.schedule.info.Task;
import com.migu.schedule.info.TaskInfo;

/**
 * 
 * @author sbt2008
 *
 */
public class TaskService {

	private Map<Integer, Task> allTask;

	/**
	 * 挂起队列
	 */
	private List<Task> hangupTaskList;

	/**
	 * 运行列表
	 */
	private List<Task> runTaskList;

	private static ServerNodeService sns;

	private static TaskService instance;

	private TaskService() {
	};

	public synchronized static TaskService getInstance() {
		if (instance != null) {
			return instance;
		}
		instance = new TaskService();
		instance.hangupTaskList = Collections.synchronizedList(new ArrayList<Task>());
		instance.runTaskList = Collections.synchronizedList(new ArrayList<Task>());
		instance.allTask = new ConcurrentHashMap<Integer, Task>();
		sns = ServerNodeService.getInstance();
		return instance;
	}

	public void clear() {
		hangupTaskList.clear();
		runTaskList.clear();
		allTask.clear();
	}

	/**
	 * 
	 * 将新的任务加到系统的挂起队列中，等待服务调度程序来调度。
	 * 
	 * 参数说明： taskId任务编号；取值范围： 大于0。 consumption资源消耗率；
	 * 
	 * 输出说明： 添加成功，返回E008任务添加成功。 如果任务编号小于等于0, 返回E009:任务编号非法。 如果相同任务编号任务已经被添加,
	 * 返回E010:任务已添加。
	 * 
	 * @param taskId
	 * @param consumption
	 * @return
	 */
	public synchronized int addTask(int taskId, int consumption) {
		if (taskId <= 0) {
			return ReturnCodeKeys.E009;
		}

		// 功率非法
		if (consumption <= 0) {
			return ReturnCodeKeys.E009;
		}

		Task task = allTask.get(taskId);

		if (task != null) {
			return ReturnCodeKeys.E010;
		}

		Task newTask = new Task(taskId, consumption);
		hangupTaskList.add(newTask);
		allTask.put(taskId, newTask);
		return ReturnCodeKeys.E008;
	}

	/**
	 * 将在挂起队列中的任务 或 运行在服务节点上的任务删除。
	 * 
	 * 参数说明： taskId任务编号；取值范围： 大于0。
	 * 
	 * 
	 * 输出说明： 删除成功，返回E011:任务删除成功。 如果任务编号小于等于0, 返回E009:任务编号非法。 如果指定编号的任务未被添加,
	 * 返回E012:任务不存在。
	 * 
	 * @param taskId
	 * @return
	 */
	public synchronized int deleteTask(int taskId) {
		if (taskId <= 0) {
			return ReturnCodeKeys.E009;
		}

		Task task = allTask.get(taskId);
		// not found
		if (task == null) {
			return ReturnCodeKeys.E012;
		}

		if (task.getNodeId() == -1) {// 说明在挂起队列
			hangupTaskList.remove(task);
			allTask.remove(task.getTaskId());
		} else {// 有服务节点
			runTaskList.remove(task);
			allTask.remove(task.getTaskId());
		}
		return ReturnCodeKeys.E011;
	}

	/**
	 * 
	 * 如果挂起队列中有任务存在，则进行根据上述的任务调度策略，获得最佳迁移方案，进行任务的迁移， 返回调度成功
	 * 如果没有挂起的任务，则将运行中的任务则根据上述的任务调度策略，获得最佳迁移方案；
	 * 如果在最佳迁移方案中，任意两台不同服务节点上的任务资源总消耗率的差值小于等于调度阈值， 则进行任务的迁移，返回调度成功，
	 * 如果在最佳迁移方案中，任意两台不同服务节点上的任务资源总消耗率的差值大于调度阈值，则不做任务的迁移，返回无合适迁移方案
	 * 
	 * 参数说明： threshold系统任务调度阈值，取值范围： 大于0；
	 * 
	 * 输出说明： 如果调度阈值取值错误，返回E002调度阈值非法。 如果获得最佳迁移方案, 进行了任务的迁移,返回E013: 任务调度成功;
	 * 如果所有迁移方案中，总会有任意两台服务器的总消耗率差值大于阈值。则认为没有合适的迁移方案,返回 E014:无合适迁移方案;
	 * 
	 * @param threshold
	 * @return
	 */
	public int scheduleTask(int threshold) {
		if (threshold < 0) {
			return ReturnCodeKeys.E002;
		}

		int totalC = 0;
		double avg = 0;
		List<Task> allList = allTaskToList();
		// 计算总功率
		for (Task task : allList) {
			totalC += task.getConsumption();
		}
		List<ServerNode> serverNodeList = sns.getServerNodeList();
		
		for (ServerNode serverNode : serverNodeList) {
			serverNode.clear();
		}
		
		avg = totalC * 1.0f / serverNodeList.size();

		Collections.sort(allList, new Comparator<Task>() {

			public int compare(Task task1, Task task2) {
				if (task1.getConsumption() - task2.getConsumption() != 0) {
					return task1.getConsumption() - task2.getConsumption();
				}
				return task2.getTaskId() - task1.getTaskId();
			}
		});
		System.out.println(allList);
		System.out.println(avg);

		// task 重新排序
		// 从大到小扔
		/*
		 * for (int i = allList.size() - 1; i >= 0; i--) { Task task =
		 * allList.get(i);
		 * 
		 * int min = Integer.MAX_VALUE; ServerNode minNode = null; //从大到小扔 for
		 * (int j = serverNodeList.size() - 1; j >= 0; j--) { // ServerNode
		 * serverNode = serverNodeList.get(j);
		 * 
		 * if(min > serverNode.getTotalConsumption()){ minNode = serverNode; min
		 * = serverNode.getTotalConsumption(); } } minNode.addTask(task); }
		 */

		/*
		 * （大任务尽可能多分小） （当任务超过平均平均值时，尽可能分大）
		 */

		for (int i = allList.size() - 1; i >= 0; i--) {
			Task task = allList.get(i);

			int min = Integer.MAX_VALUE;
			ServerNode minNode = null;

			for (int j = 0; j < serverNodeList.size(); j++) {
				//
				ServerNode serverNode = serverNodeList.get(j);

				if (min > serverNode.getTotalConsumption()) {
					minNode = serverNode;
					min = serverNode.getTotalConsumption();
				}
			}

			// 当任务超过平均平均值时，尽可能分大
			/*
			 * if (minNode.getTotalConsumption() + task.getConsumption() > avg)
			 * { // 查看所有serverNode功率， 从大node到小扔，如果大node + 超过阀值 取下一组
			 * List<ServerNode> serverNodeListByC = sns.getServerNodeListByC();
			 * for (int j = serverNodeListByC.size() - 1; j >= 0; j--) {
			 * ServerNode serverNode = serverNodeList.get(j);
			 * 
			 * if ((serverNode.getTotalConsumption() + task.getConsumption() -
			 * serverNodeListByC.get(0).getTotalConsumption()) < threshold) {
			 * minNode = serverNode; break; } } }
			 */
			minNode.addTask(task);
		}
		List<ServerNode> serverNodeListByC = sns.getServerNodeListByC();
		if ((serverNodeListByC.get(serverNodeListByC.size() - 1).getTotalConsumption()
				- serverNodeListByC.get(0).getTotalConsumption()) > threshold) {
			return ReturnCodeKeys.E014;
		}

		for (ServerNode serverNode : serverNodeList) {
			System.out.println(serverNode.getNodeId() + "  total=" + serverNode.getTotalConsumption());
		}

		return ReturnCodeKeys.E013;
	}

	private List<Task> allTaskToList() {
		ArrayList<Task> arrayList = new ArrayList<Task>();
		for (Integer taskId : allTask.keySet()) {
			arrayList.add(allTask.get(taskId));
		}
		Collections.sort(arrayList);
		return arrayList;
	}

	/**
	 * 
	 * 查询获得所有已添加任务的任务状态, 以任务列表方式返回。 参数说明： Tasks 保存所有任务状态列表；要求按照任务编号升序排列,
	 * 如果该任务处于挂起队列中, 所属的服务编号为-1; 在保存查询结果之前,要求将列表清空. 输出说明： 未做此题返回 E000方法未实现。
	 * 如果查询结果参数tasks为null，返回E016:参数列表非法 如果查询成功, 返回E015: 查询任务状态成功;查询结果从参数Tasks返回。
	 * 
	 * @param tasks
	 * @return
	 */
	public int queryTaskStatus(List<TaskInfo> tasks) {
		if (tasks == null) {
			return ReturnCodeKeys.E016;
		}
		// 清空
		tasks.clear();
		for (Integer taskId : allTask.keySet()) {
			tasks.add(allTask.get(taskId));
		}
		return ReturnCodeKeys.E015;
	}

	public Map<Integer, Task> getAllTask() {
		return allTask;
	}

	public List<Task> getHangupTaskList() {
		return hangupTaskList;
	}

	public List<Task> getRunTaskList() {
		return runTaskList;
	}

}
