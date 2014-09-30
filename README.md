## Distributed Systems coursework (Spring 2014)

The contained java file is my solution to the coursework given to the class of Spring 2014 for the Distributed Systems course at the University of Edinburgh.  The task was to create a simulator, which implemented a distributed mutual exclusion algorithm.  My solution implements the Ricart-Agrawala algorithm.  The shared resource in the coursework is an (imaginary) terminal which displays all the *print* commands given to a process. Clarifications of assumptions, methods and structures used in the solution are provided in the coursework file.

### Usage

You must compile and run the code through command line using *javac* and *java* commands.  You must also provide a *.txt* file for the input.  I have provided three examples of valid input files, namely in1, in2 and in3.  A summary of the terminology for the input files:

* begin/end process *x*, wraps the operations that process *x* must execute.
* send *y* *msg*, sends to process *y* the message, *msg*.
* recv *y* *msg*, halts the process until it has received *msg* from process *y*
* begin/end mutex, wraps the operations the process needs to execute consecutively, **i.e.** the process must sole access to the shared terminal during this time.