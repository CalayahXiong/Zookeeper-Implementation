/*
Copyright
All materials provided to the students as part of this course is the property of respective authors. Publishing them to third-party (including websites) is prohibited. Students may save it for their personal use, indefinitely, including personal cloud storage spaces. Further, no assessments published as part of this course may be shared with anyone else. Violators of this copyright infringement may face legal actions in addition to the University disciplinary proceedings.
©2022, Joseph D’Silva; ©2024, Bettina Kemme; ©2025, Olivier Michaud
*/
public class MCPi implements DistTask {
    private long samples; // Total number of samples.
    private long start; // Number of samples already done.
    private long in; // points that fell inside the circle.
    private double pi;

    // Initialize with the number of samples to be used.
    // Done at the client side.
    public MCPi(long n) {
        start = 0;
        samples = n;
        in = 0;
        pi = 0.0;
    }

    // Implementation of the DistTask interface.
    // Called at the worker side to perform the computations.
    public void compute() throws InterruptedException {
        try {
            System.out.println("DistTask: compute : started");
            pi = calcPi(samples);
            System.out.println("DistTask: compute : completed");
        } catch (InterruptedException e) {
            System.out.println("DistTask: compute : interrupted");

            throw e;
        }
    }

    private double calcPi(long n) throws InterruptedException {
        System.out.println("DistTask: calcPi : number of samples : " + n + ", start : " + start);
        for (long i = start; i < n; i++) {
            double x = Math.random(), y = Math.random();
            if (x * x + y * y <= 1) {
                // point is inside the circle
                in++;
            }
            if (Thread.interrupted()) {
                start = i;
                throw new InterruptedException("DistTask: compute was interrupted");
            }
        }
        double pival = 4.0 * ((double) in) / n;
        System.out.println("DistTask: calcPi : computed value of pi : " + pival);
        return pival;
    }

    // Called at the client side to get the computed value of pi.
    public double getPi() {
        return pi;
    }

    /*
     * public static void main(String args[])
     * {
     * long n = Long.parseLong(args[0]); // Example, pass 400000000
     * MCPi mcpi = new MCPi(n);
     * mcpi.compute();
     * System.out.println(mcpi.getPi());
     * }
     */

}