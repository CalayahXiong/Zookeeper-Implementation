This project develops a simple distributed computing platform folllowing a classical  load-balancing architecture.
There is a load-balancer process called "manager", and multiple "worker" processes.

High level objective:
A client submit a task to "/task/" for computation to the latform, which will execute the task using one of its many workers and provide the client with the result. 
Different from normal processing logics like having a middleware which receives the task from client and distributes across the workers and returs the result to the client. The manager, workers, client here are using ZooKeeper for coordination.
![zk](https://github.com/user-attachments/assets/2e598e33-2164-4e3a-8bdb-d40c69a7dcc8)
