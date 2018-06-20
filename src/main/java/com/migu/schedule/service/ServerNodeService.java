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

public class ServerNodeService {
	/**
	 * 服务节点
	 */
	public Map<Integer, ServerNode> serverNodeMap;
	
	
	private ServerNodeService(){}
	
	private static ServerNodeService instance;
	
	public static synchronized ServerNodeService getInstance(){
		if(instance != null){
			return instance;
		}
		instance = new ServerNodeService();
		instance.serverNodeMap = new ConcurrentHashMap<Integer, ServerNode>();
		return instance;
	}
	
	public void clear(){
		serverNodeMap.clear();
	}
	
	
	/**
	 * nodeId 服务节点编号, 每个服务节点全局唯一的标识, 取值范围： 大于0；
	 *
	 * 输出说明：	注册成功，返回E003:服务节点注册成功。 
	 * 			如果服务节点编号小于等于0, 返回E004:服务节点编号非法。 
	 * 			如果服务节点编号已注册,返回E005:服务节点已注册。
	 * 
	 * @param nodeId
	 * @return
	 */
	public int registerNode(int nodeId) {
		if (nodeId <= 0) {
			return ReturnCodeKeys.E004;
		}

		ServerNode serverNode = serverNodeMap.get(nodeId);
		
		//已注册
		if(serverNode != null){
			return ReturnCodeKeys.E005;
		}
		
		ServerNode newNode = new ServerNode();
		newNode.setNodeId(nodeId);
		
		serverNodeMap.put(nodeId, newNode);
		return ReturnCodeKeys.E003;
	}

	/**
	 * 
	 * 	nodeId服务节点编号, 每个服务节点全局唯一的标识, 取值范围： 大于0。
	
			输出说明：
			注销成功，返回E006:服务节点注销成功。
			如果服务节点编号小于等于0, 返回E004:服务节点编号非法。
			如果服务节点编号未被注册, 返回E007:服务节点不存在。
	 * @param nodeId
	 * @return
	 */
	public int unregisterNode(int nodeId) {
		if (nodeId <= 0) {
			return ReturnCodeKeys.E004;
		}
		ServerNode serverNode = serverNodeMap.get(nodeId);
		
		//未注册
		if(serverNode == null){
			return ReturnCodeKeys.E007;
		}
		//移除
		serverNodeMap.remove(nodeId);
		return ReturnCodeKeys.E006;
	}

	public Map<Integer, ServerNode> getServerNodeMap() {
		return serverNodeMap;
	}
	
	public List<ServerNode> getServerNodeList(){
		
		ArrayList<ServerNode> serverNodeList = new ArrayList<ServerNode>();
		for (Integer nodeId : serverNodeMap.keySet()) {
			serverNodeList.add(serverNodeMap.get(nodeId));
		}
		
		Collections.sort(serverNodeList);
		return serverNodeList;
	}
	
	public List<ServerNode> getServerNodeListByC(){
		List<ServerNode> serverNodeList = getServerNodeList();
		
		 Collections.sort(serverNodeList, new Comparator<ServerNode>() {

			public int compare(ServerNode node1, ServerNode node2) {
				return node1.getTotalConsumption()- node2.getTotalConsumption();
			}
		});
		 return serverNodeList;
	}
	
}
