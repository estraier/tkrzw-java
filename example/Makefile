# Makefile for sample programs of Tkrzw

#================================================================
# Setting Variables
#================================================================

# Generic settings
SHELL = /bin/sh

# Targets
MYCLASSES = Example1.class Example2.class Example3.class Example4.class

# Building binaries
JAVAC = javac
JAVACFLAGS = -cp .:../tkrzw.jar
JAVARUN = java
JAVARUNFLAGS = -cp .:../tkrzw.jar -Djava.library.path=.:..:/usr/local/lib

#================================================================
# Suffix rules
#================================================================

.SUFFIXES :
.SUFFIXES : .java .class

.java.class :
	$(JAVAC) $(JAVACFLAGS) $<

#================================================================
# Actions
#================================================================

all : $(MYCLASSES)

run :
	rm -rf casket*
	$(JAVARUN) $(JAVARUNFLAGS) Example1
	$(JAVARUN) $(JAVARUNFLAGS) Example2
	$(JAVARUN) $(JAVARUNFLAGS) Example3
	$(JAVARUN) $(JAVARUNFLAGS) Example4

clean :
	rm -rf $(MYCLASSES) *.class casket* *~

.PHONY : all clean

# END OF FILE
