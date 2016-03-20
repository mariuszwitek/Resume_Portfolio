# IMPORTANT! Use c++11 when running on seaquestgpvm machine
#CC11 = g++ -std=c++11
# IMPORTANT! Use c++0x when running on a gat machine
CC11 = g++ -std=c++0x 

CCFLAGS		= -O3 -Wall -ffast-math -funroll-loops

MYSQL_LIB 	= /usr/lib64/mysql
EVIO_LIB	= ../../evio
UP_LIB 		= uploader/lib
ROOT_LIB        = `root-config --libs`
ALL_LIB		= -L$(MYSQL_LIB) -L$(EVIO_LIB) -L$(UP_LIB) $(ROOT_LIB)
CCLIB		= -lm -lmysqlclient -lmyevio -lmysqlcppconn

MYSQL_INCLUDE 	= /usr/include/mysql
UP_INCLUDE 	= uploader/include
EVIO_INCLUDE 	= ../../evio
ROOT_INCLUDE    = `root-config --incdir`
ALL_INCLUDE     = -I$(MYSQL_INCLUDE) -I$(UP_INCLUDE) -I$(EVIO_INCLUDE) -I$(ROOT_INCLUDE)

ROOTCFLAGS      = `root-config --cflags`
ROOTCINT        = rootcint

all: clean maindaq-decoder 

maindaq-decoder: maindaq-decoder.o SRawEvent.o SRawEventDict.o RawStorage.o
	$(CC11) $(CCFLAGS) maindaq-decoder.o SRawEvent.o SRawEventDict.o RawStorage.o $(ALL_LIB) $(CCLIB) $(ALL_INCLUDE) -o $@ 

maindaq-decoder.o: maindaq-decoder.h docopt.c 
	$(CC11) $(CCFLAGS) -c $(ROOTCFLAGS) -o $@ maindaq-decoder.c $(ALL_LIB) $(CCLIB) $(ALL_INCLUDE)

RawStorage.o: RawStorage.h 
	$(CC11) $(CCFLAGS) -c RawStorage.cpp -o $@ -L$(MYSQL_LIB) -I$(MYSQL_INCLUDE) 

SRawEventDict.o: SRawEventDict.cxx 
	$(CC11) $(CCFLAGS) -c $(ROOTCFLAGS) SRawEventDict.cxx 

SRawEvent.o: SRawEvent.h 
	$(CC11) $(CCFLAGS) -c $(ROOTCFLAGS) SRawEvent.cxx 

SRawEventDict.cxx: SRawEvent.h SRawEventLinkDef.h
	@echo "Generating dictionary for $@ ..."
	$(ROOTCINT) -f $@ -c $^

docopt.c: maindaq-decoder.docopt docopt/template.c docopt/docopt_c.py
	python docopt/docopt_c.py -o $@ maindaq-decoder.docopt

clean: 
	rm -f maindaq-decoder.o maindaq-decoder SRawEvent.o SRawEventDict.cxx SRawEventDict.o RawStorage.o docopt.c

