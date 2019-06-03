# Simple-Distributed-Hash-Table-with-Chord-Implementation

------

![Img_1](https://raw.githubusercontent.com/ramanpreet1990/CSE_586_Distributed_HashTable_CHORD/master/Resources/1.png)


Goal
------
Implement a [**Chord**](https://en.wikipedia.org/wiki/Chord_(peer-to-peer)) based [**Distributed HashTable**](https://en.wikipedia.org/wiki/Distributed_hash_table) functionality on Android devices. Although the design is based on Chord, it is a **simplified version of Chord** i.e we do not need to implement finger tables and finger-based routing; we also do not need to handle node leaves/failures.

There are three things that have to be implemented: - 
```
1. ID space partitioning/re-partitioning
2. Ring-based routing
3. Node joins
```

**NOTE**
If there are multiple instances of the app, all instances should form a **Chord ring** and serve insert/query requests in a distributed fashion according to the Chord protocol


References
---------------
Below are the sources referred to design the DHT based on Chord: -</br>
1. [Lecture slides](http://www.cse.buffalo.edu/~stevko/courses/cse486/spring19/lectures/15-dht.pdf)</br>
2. [Chord paper](http://www.cse.buffalo.edu/~stevko/courses/cse486/spring19/files/chord_sigcomm.pdf)</br>


Writing the Content Provider
-----------------------------------------
This project implements a [**Content Provider**](https://developer.android.com/guide/topics/providers/content-providers.html) that provide all the DHT functionalities. For example, it creates server and client threads, open sockets, and respond to incoming requests based on **Chord routing protocol**.

> 1. Any app (not just our app) should be able to access (read and write) our content provider.
> 2. The content provider should **only store the < key, value > pairs local to its own partition**.
> 3. The content provider do **not need to handle concurrent node joins** and its assumed that a node join will only happen once the system completely processes the previous join.
> 4. The content provider also do **not need to handle insert/query requests while a node is joining** and its assumed  that insert/query requests will be issued only with a stable system i.e there will be **no failure** in the system.
> 5. Each content provider instance should have a node id derived from its emulator port. This node id should be obtained by applying the SHA1 hash function to the emulator port. For example, **the node id of the content provider instance running on emulator-5554 should be, node_id = genHash(“5554”)**. This is necessary to find the correct position of each node in the Chord ring.
> 6. There are always **5 nodes** in the system.
> 7. We have fixed the ports & sockets: -</br>
	a) Our app opens one server socket that listens on **Port 10000**. </br>
	b) The grading will use 5 AVDs. The redirection ports are **11108, 11112, 11116, 11120, and 11124**.



Running the Grader/Testing Program
-----------------------------------------
> 1. Load the Project in Android Studio and create the [**apk file**](https://developer.android.com/studio/run/index.html).
> 2. Download  the [**Testing Program**](https://github.com/pratik-agarwal/Simple-Distributed-Hash-Table-with-Chord-Implementation/blob/master/simpledht-grading.osx). This program works on Mac.
> 3. Before you run the program, please make sure that you are **running five AVDs**.
> 4. Also make sure that the **Emulator Networking** setup is done. 
> 5.  Run the grader: -
	- $ chmod +x ***< grader executable>***
	- $ ./*< grader executable>* ***apk file path***
> 6. **‘-h’** argument will show you what options are available. Usage is shown below: -
	-  $ *< grader executable>*  **-h**


Credits
-------
This project contains scripts and other related material that is developed by [**Networked Systems Research Group**](https://nsr.cse.buffalo.edu) at **[University of Buffalo](http://www.cse.buffalo.edu)**.

I am grateful to [**Professor Steve Ko**](https://nsr.cse.buffalo.edu/?page_id=272) for his continuous support throughout the Course ([**CSE 586**] (http://www.cse.buffalo.edu/~stevko/courses/cse486/spring19/)) that helped me learn the skills of Large Scale Distributed Systems and develop a simplified version of **Distributed HashTable based on CHORD protocol**.
