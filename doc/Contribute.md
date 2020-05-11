

# How to contribute to BigGrph

## What is BigGrph ?

BigGrph is a library for distributed graph processing running over clusters. 

## Tool setup

BigGrph development is done using Eclipse, and the source version management uses a Subversion repository. The
setup of necessary tools requires at least these two software.


### Eclipse

Use the current version of [Eclipse IDE](https://eclipse.org/downloads/eclipse-packages/) for Java development.

### Subversion

Setup subversion inside eclipse, using either [Subclipse](http://subclipse.tigris.org/) or Subversive.


## Getting the sources

Sources of BigGrph are stored in a Subversion repository located on the [INRIA forge](http://gforge.inria.fr).
BigGrph is made of several Eclipse projects, the complete set of sources can be retrieved with a checkout of
`svn+ssh://scm.gforge.inria.fr/svnroot/dipergrafs/biggrph/trunk/` 

```bash
svn co svn+ssh://scm.gforge.inria.fr/svnroot/dipergrafs/biggrph/trunk/
```

Then import all the projects in the Eclipse workspace.

Some Eclipse subversion plugins allow to perform both the checkout and the import of all the Eclipse
projects found in the set of retrieved sources. 

## Brief description of the projects

1. BigGrph

   BigGrph is the toplevel project containing all classes used to implement graphs and
   algorithms on graphs.

2. biggrph.examples

   Contains several example main programs for algorithms implemented in BigGrph.
   These programs can be used both in a standalone way or through a cluster
   scheduling system : Torque/PBS and OAR are currently supported.
   
3. client-server

   Contains an abstract implementation of a server listening on a TCP socket. This class is 
   the base class of all the various TCP servers used in BigGrph.

4. dependencies

   Gathers jars of external dependencies used by BigGrph. These jar dependencies are used
   by the build process of Eclipse.
    
5. jacaboo

   This project contains all the classes used to run BigGrph applications on various cluster
   systems, using SSH as the launch tool for the program running on the cluster nodes.
   
6. java4unix

7. ldjo

   Is a distributed object framework. It allows the creation of objects distributed over all
   the nodes of a cluster, and to invoke services between such objects.

8. MobileAgentInfrastructure

9. octojus

10. tools

## Compiling with Maven

Outside of Eclipse, it is possible to compile BigGrph using Maven. The maven parent project
is the bigGrph project. To compile, change the current directory to the bigGrph project and
execute Maven:

```bash
cd bigGrph && mvn clean package
```

The previous command launches all the unit tests. Is is possible to skip this phase using the
following Maven command:

```bash
mvn clean package -Dmaven.test.skip.exec=true
```

Building with maven produces JAR archives in the target/ directory of all projects. All JARs
include all dependencies, both internal and external. The two most useful JAR archives are:

1. the one in the bigGrph project (`bigGrph/target/biggrph-1.0-SNAPSHOT.jar`) contains all the
   classes needed to build your own applications using BigGrph.

2. the one in the biggrph.examples project (`biggrph.examples/target/biggrph.examples-1.0-SNAPSHOT.jar`)
   includes all the classes of BigGrph plus the example programs. 

In both of these projects, the `target/` directory also contains all the source files gathered in a 
single JAR file: `biggrph-1.0-SNAPSHOT-sources.jar` for BigGrph and `biggrph.examples-1.0-SNAPSHOT-sources.jar`
for BigGrph plus the example programs.
