# DDM - IDD

This is a scala project that solves the Inclusion Dependency Discovery exercises for 
the Distributed Data Management lecture @ HPI in the winter semester 2019/20.

The solution builds a simple Spark pipeline that is structured after the SINDY algorithm proposed by Sebastian Kruse, Thorsten Papenbrock and Felix Naumann in
their 2015 paper: Scaling Out the Discovery of Inclusion Dependencies (https://hpi.de/fileadmin/user_upload/fachgebiete/naumann/publications/2015/Scaling_out_the_discovery_of_INDs-CR.pdf)

### Performance
On the local Windows machine with 4 cores, where the application was tested, the execution time to find
all IDD's in the TPCH dataset ("nation", "region", "supplier", "customer", "part", "lineitem", "orders") was ~ 45 seconds.
