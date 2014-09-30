package submit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;

/** INFR 11022: Distributed Systems - Coursework Assignment, Programming part.
 * @author s1035578
 *
 */

/** README:
 *  This code implements the Ricart-Agrawala algorithm for mutual exclusion.
 *  It ONLY updates its own logical clock through send/receive instructions.
 *  All the coursework code is on this single file.  This file has two classes:
 *  the main Coursework class, and the DistProcess class, which represents a
 *  process.
 *  
 *  It has the following ASSUMPTIONS:
 *   - No process fails during execution of the simulator.
 *   - No process sends messages to inexisting processes.
 *   - No process is given the same name.
 *   - Each process can see what processes have finished.
 *   
 *  The simulator is designed in the following ways:
 *   - It implements the Ricart-Agrawala algorithm for mutual exclusion.
 *   - It is a multi-threaded simulator, i.e. each process is represented 
 *     by a thread object, called DistProcess.  DistProcess is an object class
 *     which extends the thread class of Java.
  *   - It parses the input file, using the code given in the Coursework
 *     description file, with adjustments.
 *   - For a `print' command in the input file which is not enclosed in a mutex
 *     block, then the parser will add an enclosing mutex block.  This is for
 *     the simulator to recognise each individual `print' command requires 
 *     the critical section. 
 *   - Each DistProcess keeps an internal logical clock, which is incremented
 *     with calls to the sendMsg, recvMsg and printMsg methods.
 *   - Each DistProcess ONLY updates its internal clock upon call of the recvMsg
 *     method, i.e. when it receives a message and NOT when it receives an 
 *     approval.
 *   - ConcurrentLinkedQueue objects are used to simulate the communication 
 *     channels.  If one process wants to send a message to another, it adds 
 *     the message (an array of Strings) to the ConcurrentLinkedQueue belonging
 *     to the process it wants to send to.  This each process has one to
 *     represent its message queue.
 *   - Each DistProcess has three queues: a message queue as mentioned above; a 
 *     task queue (LinkedList object) which stores all the tasks; and a queue
 *     of approvals (LinkedList object) which stores all requests for approvals
 *     received while the process was in the critical section or it had a later
 *     timestamp.
 *   - After the file is parsed, every processes approvals array is initialised.
 *     The process starts afterwards.  The need to initialise the approvals 
 *     array in this way, rather than in the object constructor is that the
 *     number of all processes is unknown until the end.
 *   - Each process runs until it is finished.  It finishes when it processes
 *     the command "end process".  When it finishes, it will also process all
 *     requests for the critical section. 
 *   - When waiting for approval, the process will check whether there are 
 *     processes that have finished, and thus update their approval array as
 *     appropriate, i.e. it does not wait for a response to a request it sent
 *     to a finished process, as it might not get one.  This is mainly a safety 
 *     measure, as it may be that the finished process did not see the request
 *     before it finished.
 *     
 *   TO RUN THE CODE:
 *   - Compile this file, namely "javac Coursework.java" on terminal.
 *   - Run the class file with the given input file, 
 *     example: "java Coursework in1.txt"
 */

public class Coursework {
	
	private static Coursework c;
	private static ArrayList<DistProcess> allProcesses;	//ArrayList containing all the processes
	private static HashMap<String, ConcurrentLinkedQueue<String[]>> msgQueues;	//Each processes msg queue
	private static HashMap<Integer, Boolean> processesFinished;	//Global resource displaying which process has finished
	
	/** Parses the input file, and builds up the task queues for each process.
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		c = new Coursework();
		BufferedReader in = new BufferedReader(new FileReader(args[0]));
		String line = null;
		
		//Set up variables
		DistProcess currentProcess = null;
		int currentProcessNo = 0;
		allProcesses = new ArrayList<DistProcess>();
		msgQueues = new HashMap<String, ConcurrentLinkedQueue<String[]>>();
		processesFinished = new HashMap<Integer, Boolean>();
		boolean processInMutex = false;

		while ((line = in.readLine()) != null) {

			String[] arr = line.trim().split("[ ]+");

			if (arr[0].equals("begin")) {
				if (arr[1].equals("process")) {
					//Start up a new DistProcess
					ConcurrentLinkedQueue<String[]> msgQueue = new ConcurrentLinkedQueue<String[]>();
					msgQueues.put(arr[2], msgQueue);
					processesFinished.put(currentProcessNo, false);
					currentProcess = c.new DistProcess(msgQueue, arr[2], currentProcessNo);
					allProcesses.add(currentProcess);
					currentProcessNo++;
					processInMutex = false;
				} else if (arr[1].equals("mutex")) {
					processInMutex = true;
					currentProcess.addTask(arr);
				}
			}

			if (arr[0].equals("end")) {
				if (arr[1].equals("process")) {
					currentProcess.addTask(arr);
				} else if (arr[1].equals("mutex")) {
					processInMutex = false;
					currentProcess.addTask(arr);
				}
			}

			if (arr[0].equals("send")) {
				currentProcess.addTask(arr);
			}

			if (arr[0].equals("recv")) {
				currentProcess.addTask(arr);
			}

			if (arr[0].equals("print")) {
				if (processInMutex)
					currentProcess.addTask(arr);
				else {
					currentProcess.addTask(new String[]{"begin","mutex"});
					currentProcess.addTask(arr);
					currentProcess.addTask(new String[]{"end","mutex"});
				}
			}
		}

		in.close();
		
		//Initialises all approval lists
		for (int i=0;i<allProcesses.size();i++) {
			allProcesses.get(i).initApprovalList(allProcesses.size(), i);
		}
		
		//Start all processes
		for (int i=0;i<allProcesses.size();i++) {
			allProcesses.get(i).start();
		}

	}
	
	/** DistProcess is the object class representing the processes of the simulator.
	 *  It extends the java thread class.
	 */
	private class DistProcess extends Thread {

		private ConcurrentLinkedQueue<String[]> msgQueue;	//This queue simulates the communication channel 
		private String id;	//id of the process
		private int processNo;	//Used for approvals purposes
		private int internalClock = 0;	//Process's logical clock
		
		private LinkedList<String[]> taskQueue;	//All tasks for this process are put in this queue
		
		private ArrayList<String> approvalQueue;	//Queue of requests of approvals from other processes
		
		private boolean[] approvals;	//Record the approvals received from other processes
		
		private boolean waitingToReceive = false;	//Identify whether process is waiting to receive a msg
		private String receiveFrom;	//Which process should this process receive from
		private String receiveMsg;
		
		private boolean waitingForApproval = false;	//Identify whether process is waiting to be approved into crit sect
		private boolean inCritSect = false;	//Identify whether this process is in crit sect
		
		private boolean finished;	//Status of the process
		
		//Constructor
		public DistProcess(ConcurrentLinkedQueue<String[]> msgQueue, String id, int processNo) {
			this.msgQueue = msgQueue;
			this.id = id;
			this.taskQueue = new LinkedList<String[]>();
			this.approvalQueue = new ArrayList<String>();
			this.processNo = processNo;
			this.finished = false;
		}
		
		@Override
		public void run() {
			
			while (!finished) {
				if (waitingToReceive) {	//Currently waiting to receive a msg, so blocks until received
					if (!msgQueue.isEmpty()) {
						String[] msg = msgQueue.poll();
						if (msg[0].equals(receiveFrom) && msg[1].equals(receiveMsg)) {
							waitingToReceive = false;
							recvMsg(msg);	//Got the right msg
						} else if (msg[0].equals("request")) {	//Msg was a request
							if (inCritSect)
								approvalQueue.add(msg[1]);
							else
								sendApproval(msg[1]);
						} else
							msgQueue.add(msg);
					}
				} else if (waitingForApproval) {	//Currently waiting to get into crit sect, so blocks until in
					approvals = updateApprovalList(approvals);
					
					if (!msgQueue.isEmpty()) {
						String[] msg = msgQueue.poll();
						if (msg[0].equals("approve")) {
							int recvProcessNo = Integer.parseInt(msg[1]);
							approvals[recvProcessNo] = true;
						} else if (msg[0].equals("request")) {
							int recvTimestamp = Integer.parseInt(msg[3]);
							if (internalClock > recvTimestamp) {	//If this process has later time
								sendApproval(msg[1]);
							} else if (internalClock == recvTimestamp) {
								//Check who has smallest processNo, that process gets the crit sect
								if (processNo < Integer.parseInt(msg[2]))
									approvalQueue.add(msg[1]);
								else
									sendApproval(msg[1]);
							} else
								approvalQueue.add(msg[1]);
						} else
							msgQueue.add(msg);  //Some process sent something, that is not an approval, add it back to queue
					}
					if (allApproved()) {
						waitingForApproval = false;
						inCritSect = true;
					}
				} else {  //Not waiting for approval or waiting to receive, so do some tasks
					if (!msgQueue.isEmpty()) {  //Check for some msgs first
						String[] msg = msgQueue.poll();
						if (msg[0].equals("request")) {
							if (inCritSect)
								approvalQueue.add(msg[1]);
							else
								sendApproval(msg[1]);
						} else  //Not an approval request, so leave it in queue
							msgQueue.add(msg);
					}
					if (!taskQueue.isEmpty()) {  //Do some task
						String[] command = taskQueue.pop();
						if (command[0].equals("send")) {
							ConcurrentLinkedQueue<String[]> receiverMsgQueue = findMsgQueue(command[1], msgQueues);
							sendMsg(receiverMsgQueue, command[1], command[2]);
						}
						if (command[0].equals("recv")) {
							waitingToReceive = true;	//Go to blocking to receive mode
							receiveFrom = command[1];
							receiveMsg = command[2];
						}
						if (command[0].equals("print")) {
							printMsg(command[1]);
						}
						if (command[0].equals("begin") && command[1].equals("mutex")) {
							requestMutex();
						}
						if (command[0].equals("end") && command[1].equals("mutex")) {
							inCritSect = false;	//Get out of critical section
							initApprovalList(allProcesses.size(), processNo);
							replyToAllQueuedApprovals();	//Approve all requests received while in critical section
						}
						if (command[0].equals("end") && command[1].equals("process")) {
							finished = true;	//This process has finished
							if (!msgQueue.isEmpty()) {
								for (int i=0;i<msgQueue.size();i++) {
									String[] msg = msgQueue.poll();
									if (msg[0].equals("request")) {	//Send an approval to all requests
										sendApproval(msg[1]);
									}
								}
							}
							processesFinished.put(processNo, true);	//Update global resource, to say this process finished.
						}

					}
				}	
			}
		}
		
		//Method which sends a given msg to a given process and prints for the 
		// `send' command
		private void sendMsg(ConcurrentLinkedQueue<String[]> receiverMsgQueue, String recvId, String msg) {
			internalClock++;	//Increment internal logical clock
			String[] totalMsg = new String[] {id, msg, String.valueOf(internalClock)};
			System.out.printf("sent %s %s %s %d \n", id, msg, recvId, internalClock);
			receiverMsgQueue.add(totalMsg);
		}
		
		//Method that prints for `recv' command.  It gets called when we have 
		// received the correct msg
		private void recvMsg(String[] totalMsg) {
			int senderClock = Integer.parseInt(totalMsg[2]);
			if (senderClock>internalClock)
				internalClock = senderClock;
			internalClock++;	//Increment internal logical clock
			
			System.out.printf("received %s %s %s %d \n", id, totalMsg[1], totalMsg[0], internalClock);
		}
		
		//Method that prints for `send' command.
		private void printMsg(String msg) {
			internalClock++;	//Increment internal logical clock
			System.out.printf("printed %s %s %d \n", id, msg, internalClock);
		}
		
		//Method that requests for a mutex, by sending all other processes a 
		// request msg
		private void requestMutex() {
			for (int i=0;i<allProcesses.size();i++) {
				if (!allProcesses.get(i).equals(this)) {
					String processId = allProcesses.get(i).getProcessId();
					ConcurrentLinkedQueue<String[]> recvMsgQueue = msgQueues.get(processId);
					String[] request = new String[]{"request", id, String.valueOf(processNo), String.valueOf(internalClock)};
					recvMsgQueue.add(request);
				}
			}
			waitingForApproval = true;
		}
		
		//Method which sends approval to the specified receiving process
		private void sendApproval(String receiver) {
			ConcurrentLinkedQueue<String[]> recvMsgQueue = msgQueues.get(receiver);
			
			String[] approval = new String[] {"approve", String.valueOf(processNo), String.valueOf(internalClock)};
			recvMsgQueue.add(approval);
		}
		
		//Method which is called when exiting crit sect.  Sends to an approval 
		// to all processes that asked for approval, whilst this process was 
		// in crit sect
		private void replyToAllQueuedApprovals() {
			for (int i=0;i<approvalQueue.size();i++) {
				String request = approvalQueue.get(i);
				sendApproval(request);
			}
			approvalQueue = new ArrayList<String>();
		}
		
		//Called by the parser, adds a given task to process' task queue.
		public void addTask(String[] task) {
			taskQueue.add(task);
		}
		
		//Return id
		public String getProcessId() {
			return id;
		}
		
		//Initialises the list of approvals.  Called each time exiting crit sect.
		private void initApprovalList(int numberOfProcesses, int numberInList) {
			this.approvals = createApprovalList(numberOfProcesses, numberInList);
		}
		
		//Checks if received all the approvals.
		private boolean allApproved() {
			for (int i=0;i<approvals.length;i++) {
				if (!approvals[i])
					return false;
			}
			return true;
		}
	}
	
	//Method which returns the correct msg queue
	public static ConcurrentLinkedQueue<String[]> findMsgQueue(String processId, HashMap<String,ConcurrentLinkedQueue<String[]>> msgQueues) {
		return msgQueues.get(processId);
	}
	
	//Creates an approval list.
	public boolean[] createApprovalList(int numberOfProcesses, int indicator) {
		boolean[] approvalList = new boolean[numberOfProcesses];
		approvalList[indicator] = true;
		return approvalList;
	}
	
	//Updates a given approval list.  This is to ensure finished processes have
	// been taken into account.
	public boolean[] updateApprovalList(boolean[] approvals) {
		for (int i=0;i<approvals.length;i++) {
			if (processesFinished.get(i))
				approvals[i] = true;
		}
		return approvals;
	}
}
