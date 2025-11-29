/*
Copyright
All materials provided to the students as part of this course is the property of respective authors. Publishing them to third-party (including websites) is prohibited. Students may save it for their personal use, indefinitely, including personal cloud storage spaces. Further, no assessments published as part of this course may be shared with anyone else. Violators of this copyright infringement may face legal actions in addition to the University disciplinary proceedings.
©2022, Joseph D’Silva; ©2024, Bettina Kemme; ©2025, Olivier Michaud
*/
import java.io.*;

import java.util.*;

// To get the name of the host.
import java.net.*;

//To get the process id.
import java.lang.management.*;
import java.util.concurrent.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException.*;
import org.apache.zookeeper.data.Stat;

// TODO
// Replace XX with your group number.
// You may have to add other interfaces such as for threading, etc., as needed.
// This class will contain the logic for both your manager process as well as the worker processes.
//  Make sure that the callbacks and watch do not conflict between your manager's logic and worker's logic.
//		This is important as both the manager and worker may need same kind of callbacks and could result
//			with the same callback functions.
//	For simplicity, so far all the code in a single class (including the callbacks).
//		You are free to break it apart into multiple classes, if that is your programming style or helps
//		you manage the code more modularly.
//	REMEMBER !! Managers and Workers are also clients of ZK and the ZK client library is single thread - Watches & CallBacks should not be used for time consuming tasks.
//		In particular, if the process is a worker, Watches & CallBacks should only be used to assign the "work" to a separate thread inside your program.
public class DistProcess implements Watcher, AsyncCallback.ChildrenCallback {
    ZooKeeper zk;
    String zkServer, pinfo;
    boolean isManager = false; //either manager or worker
    boolean initialized = false;

    // dist
    String basePath = "/dist04";
    String tasksPath;
    String workersPath;
    String managerPath;
    String assignmentsPath;
    String resultsPath;


    long timeSlice = 500;

    // how to detect the status of worker? can watcher do this?
    ConcurrentHashMap<String, String> workers; //if I'm a manager, keep info of workerID - status
    String myWorkerID;

    ExecutorService workerExecutor;

    void initPaths() {
        tasksPath = basePath + "/tasks";
        workersPath = basePath + "/workers";
        managerPath = basePath + "/manager";
        assignmentsPath = basePath + "/assignments";
        resultsPath = basePath + "/results";
    }

    DistProcess(String zkhost) {
        zkServer = zkhost;
        pinfo = ManagementFactory.getRuntimeMXBean().getName();
        System.out.println("DISTAPP : ZK Connection information : " + zkServer);
        System.out.println("DISTAPP : Process information : " + pinfo);
    }

    void startProcess() throws IOException, UnknownHostException, KeeperException, InterruptedException {
        zk = new ZooKeeper(zkServer, 10000, this); //connect to ZK.
        initPaths();
    }

    void initialize() {
        try {
            ensureBaseNodes();
            workers = new ConcurrentHashMap<>();

            runForManager();    // See if you can become the manager (i.e, no other manager exists)
            isManager = true;

            System.out.println("I'm the manager");

            watchWorkers(); // TODO monitor for worker tasks?

            getTasks(); // Install monitoring on any new tasks that will be created.

        } catch (NodeExistsException nee) {
            isManager = false;

            System.out.println("I'm a worker");

            registerForWorker();

            workerExecutor = Executors.newSingleThreadExecutor();

        } // TODO: What else will you need if this was a worker process?
        catch (UnknownHostException uhe) {
            System.out.println(uhe);
        } catch (KeeperException ke) {
            System.out.println(ke);
        } catch (InterruptedException ie) {
            System.out.println(ie);
        }

        System.out.println("DISTAPP : Role : " + " I will be functioning as " + (isManager ? "manager" : "worker"));
    }

    void ensureBaseNodes() {
        try {
            zk.create(basePath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException | InterruptedException e) {
            System.out.println(e);
        }
        try {
            zk.create(tasksPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException | InterruptedException e) {
            System.out.println(e);
        }
        try {
            zk.create(workersPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException | InterruptedException e) {
            System.out.println(e);
        }
        try {
            zk.create(assignmentsPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException | InterruptedException e) {
            System.out.println(e);
        }
        try {
            zk.create(resultsPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException | InterruptedException e) {
            System.out.println(e);
        }
    }

    void registerForWorker() {
        try {
            myWorkerID = pinfo.replace('@', '-'); // example: jxiong3-tr-open-14.cs.mcgill.ca
            String myWorkerPath = workersPath + "/" + myWorkerID;

            // create an ephemeral worker node for me.
            try {
                zk.create(
                        myWorkerPath,
                        "IDLE".getBytes(),
                        Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL
                );
            } catch (NodeExistsException e) { // one machine multiple worker threads?
                // append random suffix if needed.
                myWorkerID = myWorkerID + "-" + new Random().nextInt(10000);
                myWorkerPath = workersPath + "/" + myWorkerID;
                zk.create(myWorkerPath, "IDLE".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            }
            System.out.println("Worker registered in: " + myWorkerPath);

            // create a persistent assignment node for this worker.
            String myAssignmentPath = assignmentsPath + '/' + myWorkerID;
            try {
                zk.create(
                        myAssignmentPath,
                        new byte[0],
                        Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT
                );
            } catch (NodeExistsException ne) {
            }
            System.out.println("Assignment node ready: " + myAssignmentPath);
            watchMyAssignment(); // start worker Watcher for its assignment.
        } catch (Exception e) {
            System.out.println("Worker registeration failed: " + e);
        }
    }

    // Manager fetching task znodes...
    void getTasks() {
        zk.getChildren(tasksPath, this, new AsyncCallback.ChildrenCallback() {

            @Override
            public void processResult(int i, String s, Object o, List<String> tasks) {
                if (!isManager || tasks == null) return;
                for (String taskID : tasks) {
                    // install data watch and read data asyn in case of worker write it back.
                    assignTaskToWorker(taskID);
                }
            }
        }, null);
    }

    void assignTaskToWorker(String taskID) {
        try {
            String workerID = pickIdleWorker();
            if (workerID == null) {
                System.out.println("No IDLE workers, task left unassigned: " + taskID);
                return;
            }

            byte[] data = zk.getData(tasksPath + "/" + taskID, false, null);
            String assignPath = assignmentsPath + "/" + workerID + "/" + taskID;
            try {
                zk.create(assignPath, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (NodeExistsException nee) {
                // shouldn't usually happen; overwrite
                zk.setData(assignPath, data, -1);
            }

            String oldTaskPath = tasksPath + "/" + taskID;
            try {
                zk.delete(oldTaskPath, -1);
            } catch (KeeperException.NoNodeException nne){
                System.out.println(nne);
            }
            System.out.println("Task: " + taskID + " assigned to worker: " + workerID + " and removed from /tasks");

            String workerStatusPath = workersPath + "/" + workerID;
            try {
                zk.setData(workerStatusPath, "BUSY".getBytes(), -1);
            } catch (KeeperException.NoNodeException nne) {
                System.out.println("Worker: " + workerID + " disappeared before assignment accomplished.");
                workers.remove(workerID);
            }

        } catch (KeeperException | InterruptedException e) {
            System.out.println(e);
        }
    }

    String pickIdleWorker()
    {
        if (workers.isEmpty()) return null;
        for (String workerID : workers.keySet()){
            if(workers.get(workerID).equals("IDLE")) return workerID;
        }
        return null;
    }

    // Listen to new registered worker.
    void watchWorkers()
    {
        zk.getChildren(workersPath, this, new AsyncCallback.ChildrenCallback(){
            @Override
            public void processResult(int i, String s, Object o, List<String> workerList) {
                if(!isManager | workerList == null) return;

                // update worker list.
                Set<String> currentWorkers = new HashSet<>(workerList);
                for (String w : new HashSet<>(workers.keySet())) {
                    if(!currentWorkers.contains(w)) {
                        workers.remove(w);
                    }
                }
                // update workers' status.
                for (String w : workerList) {
                    final String workerPath = workersPath + "/" + w;
                    zk.getData(workerPath, true, new AsyncCallback.DataCallback(){

                        @Override
                        public void processResult(int i, String s, Object o, byte[] data, Stat stat) {
                            String status = "IDLE";
                            if (data != null) status = new String(data);
                            workers.put(w, status);
                        }
                    },null);
                }
                System.out.println("Manager: Active workers = " + workerList);
                // In case there are new changes to the children of the workers node.
                // Manager should re-install the Watch to assign the task
                // getTasks();
            }
        }, null);
    }

    // Worker fetching assignment znodes...
    // /dist04/assignments/workerID
    void watchMyAssignment()
    {
        String myAssignmentPath = assignmentsPath + "/" + myWorkerID;
        // listen task-xxx.
        zk.getChildren(myAssignmentPath, this, new AsyncCallback.ChildrenCallback() {
            @Override
            public void processResult(int rc, String p, Object ctx, List<String> children) {

                // children contains 0/1 task.
                if(children == null || children.isEmpty()) return; // no assigned task.
                System.out.println("A new task is added under my assignment: " + children.get(0));
                getMyAssignedTask(children.get(0)); // fetch data & execute.
            }
        }, null);
    }

    void getMyAssignedTask(String taskID)
    {
        // switch my status to "busy". And this should be written by manager already.
        String myStatusPath = workersPath + "/" + myWorkerID;
        try {
            zk.setData(myStatusPath, "BUSY".getBytes(), -1);
        } catch (Exception statusEx) {
            System.out.println("Failed in updating worker status: " + statusEx);
        }

        // fetch task data.
        String myAssignedTasks = assignmentsPath + "/" + myWorkerID + "/" + taskID;
        zk.getData(myAssignedTasks, false, new AsyncCallback.DataCallback() {

            @Override
            public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {

                // in case the task is null.
                if (bytes == null) {
                    System.out.println("Assignment node has no data.");

                    try { zk.delete(myAssignedTasks, -1); }
                    catch (Exception e) {
                        e.printStackTrace();}

                    try { zk.setData(myStatusPath, "IDLE".getBytes(), -1); }
                    catch (Exception e) {
                        e.printStackTrace();
                    }

                    return;
                }

                DistTask dt = deserialize(bytes);
                workerExecutor.submit(() -> {
                    // execute task in worker thread.
                    try {
                        runForTimeSlice(taskID, dt, myStatusPath); // during which interruption may happen.
                    }

                    // clean assignment node and update my status.
                    finally {
                        try {
                            zk.delete(myAssignedTasks, -1);
                            System.out.println("I deleted it from my assignment.");
                        } catch (Exception ex) {System.out.println(ex);}

                        // set worker status to "IDLE"
                        try {
                            zk.setData(myStatusPath, "IDLE".getBytes(), -1);
                        } catch (Exception ex) {System.out.println(ex);}

                        watchMyAssignment(); // watch for the next assignment.
                    }
                });
            }
        }, null);
    }

    // process result: either writeTaskBack or returnResult.
    public void runForTimeSlice(String taskID, DistTask dt, String statusPath) {

        Thread computeThread = new Thread(() -> {
            try {
                dt.compute();
            } catch (InterruptedException e) {
                System.out.println("Compute interrupted for task " + taskID);
            }
        });

        computeThread.start();

        try {
            // Wait for at most time slice until it's interrupted.
            computeThread.join(timeSlice);
        } catch (InterruptedException e) {
            computeThread.interrupt();
        }

        try {
            // times out but task isn't processed completely.
            if (computeThread.isAlive()) {

                System.out.println("Time slice ended for task: " + taskID);
                computeThread.interrupt();

                writeTaskBack(taskID, dt);

                return;
            }
            // successful completing the assigned task.
            System.out.println("Task " + taskID + " finished!");
            returnResult(taskID, dt);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Re-construct our task object.
    DistTask deserialize(byte[] data)
    {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bis)) {

            return (DistTask) ois.readObject();

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    // After the worker completes the compute(), it must write the result into /dist04/results/task-xxx
    void returnResult(String taskID, DistTask task) throws InterruptedException, KeeperException {
        byte[] data = serialize(task);
        String resultPath = resultsPath + "/" + taskID;
        try {
            zk.create(resultPath, data, Ids.OPEN_ACL_UNSAFE,  CreateMode.PERSISTENT);
        } catch (NodeExistsException e) {
            zk.setData(resultPath, data, -1);
        }
        catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    byte[] serialize(DistTask task)
    {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);

            oos.writeObject(task);
            oos.flush();

            return bos.toByteArray();

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    // write the partial result to tasks/task-xxx
    void writeTaskBack(String taskID, DistTask dt)
    {
        String taskPath = tasksPath + "/" + taskID;
        try {
            zk.create(taskPath, serialize(dt), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (NodeExistsException e) {
            try {
                zk.setData(taskPath, serialize(dt), -1);
            } catch ( InterruptedException | KeeperException ex) {
                ex.printStackTrace();
            }
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    // Try to become the manager.
    void runForManager() throws UnknownHostException, KeeperException, InterruptedException
    {
        //Try to create an ephemeral node to be the manager, put the hostname and pid of this process as the data.
        // This is an example of Synchronous API invocation as the function waits for the execution and no callback is involved..
        zk.create(managerPath, pinfo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("Manager ephemeral node has been created: " + managerPath);
    }

    public void process(WatchedEvent e)
    {
        //Get watcher notifications.

        //!! IMPORTANT !!
        // Do not perform any time consuming/waiting steps here
        //	including in other functions called from here.
        // 	Your will be essentially holding up ZK client library
        //	thread and you will not get other notifications.
        //	Instead include another thread in your program logic that
        //   does the time consuming "work" and notify that thread from here.

        System.out.println("DISTAPP : Event received : " + e);

        if(e.getType() == Watcher.Event.EventType.None) // This seems to be the event type associated with connections.
        {
            // Once we are connected, do our initialization stuff.
            if(e.getPath() == null && e.getState() ==  Watcher.Event.KeeperState.SyncConnected && initialized == false)
            {
                initialize();
                initialized = true;
            }
        }

        if(isManager) {
            // Manager should be notified if any new znodes are added to tasks.
            if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged && e.getPath().equals(tasksPath)) {
                // There has been changes to the children of the node.
                // We are going to re-install the Watch as well as request for the list of the children.
                getTasks();
            }
            // Manager should be notified if any new znodes are added to workers.
            // Manager should be notified when worker become IDLE then update workers.
            if ((e.getType() == Event.EventType.NodeChildrenChanged && e.getPath().equals(workersPath)) ||
                    e.getType() == Event.EventType.NodeDataChanged && e.getPath().startsWith(workersPath + "/")){
                watchWorkers();
            }
        } else { // Worker should be notified if any new znodes related to it are added to assignments by manager.
            if( e.getType() == Event.EventType.NodeChildrenChanged && e.getPath().equals(assignmentsPath + "/" + myWorkerID)){
                watchMyAssignment();
            }
        }
    }

    //Asynchronous callback that is invoked by the zk.getChildren request.
    public void processResult(int rc, String path, Object ctx, List<String> children)
    {

        //!! IMPORTANT !!
        // Do not perform any time consuming/waiting steps here
        //	including in other functions called from here.
        // 	Your will be essentially holding up ZK client library
        //	thread and you will not get other notifications.
        //	Instead include another thread in your program logic that
        //   does the time consuming "work" and notify that thread from here.

        // This logic is for manager !!
        //Every time a new task znode is created by the client, this will be invoked.

        // TODO: Filter out and go over only the newly created task znodes.
        //		Also have a mechanism to assign these tasks to a "Worker" process.
        //		The worker must invoke the "compute" function of the Task send by the client.
        //What to do if you do not have a free worker process?
//        System.out.println("DISTAPP : processResult : " + rc + ":" + path + ":" + ctx);
//        for(String c: children)
//        {
//            System.out.println(c);
//            try
//            {
//                //TODO There is quite a bit of worker specific activities here,
//                // that should be moved done by a process function as the worker.
//
//                //TODO!! This is not a good approach, you should get the data using an async version of the API.
//                byte[] taskSerial = zk.getData("/dist04/tasks/"+c, false, null);
//
//                // Re-construct our task object.
//                ByteArrayInputStream bis = new ByteArrayInputStream(taskSerial);
//                ObjectInput in = new ObjectInputStream(bis);
//                DistTask dt = (DistTask) in.readObject();
//
//                //Execute the task.
//                //TODO: Again, time consuming stuff. Should be done by some other thread and not inside a callback!
//                dt.compute();
//
//                // Serialize our Task object back to a byte array!
//                ByteArrayOutputStream bos = new ByteArrayOutputStream();
//                ObjectOutputStream oos = new ObjectOutputStream(bos);
//                oos.writeObject(dt); oos.flush();
//                taskSerial = bos.toByteArray();
//
//                // Store it inside the result node.
//                zk.create("/dist04/tasks/"+c+"/result", taskSerial, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//                //zk.create("/distXX/tasks/"+c+"/result", ("Hello from "+pinfo).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//            }
//            catch(NodeExistsException nee){System.out.println(nee);}
//            catch(KeeperException ke){System.out.println(ke);}
//            catch(InterruptedException ie){System.out.println(ie);}
//            catch(IOException io){System.out.println(io);}
//            catch(ClassNotFoundException cne){System.out.println(cne);}
//        }
    }


    public static void main(String args[]) throws Exception
    {
        //Create a new process
        //Read the ZooKeeper ensemble information from the environment variable.
        DistProcess dt = new DistProcess(System.getenv("ZKSERVER"));
        dt.startProcess();

        //Replace this with an approach that will make sure that the process is up and running forever.
        //Thread.sleep(20000);
        new CountDownLatch(1).await();
    }
}