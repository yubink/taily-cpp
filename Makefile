TAILY_TARGET=Taily
INDRI_TARGET=TailyRunQuery
DOCDUMP_TARGET=DumpDocVec
COMMON_SOURCES=FeatureStore.cpp ShardRanker.cpp
TAILY_SOURCES=Main.cpp
INDRI_SOURCES=TailyRunQuery.cpp
DOCDUMP_SOURCES=DumpDocVec.cpp
CXX=g++

ifdef OLDINDRI
	DEPENDENCIES = lemur xpdf antlr
	INCPATH=-I$(HOME)/indri-5.2/include $(patsubst %, -I$(HOME)/indri-5.2/contrib/%/include, $(DEPENDENCIES))   -I$(HOME)/include
	LDFLAGS=-g -L$(HOME)/indri-5.2/obj $(patsubst %, -L$(HOME)/indri-5.2/contrib/%/obj, $(DEPENDENCIES)) -L$(HOME)/opt/db-6.0.20.NC/build_unix -ldb_cxx -lindri $(patsubst %, -l%, $(DEPENDENCIES)) -lz -lpthread -lm
else
	INCPATH=-I$(HOME)/include 
	LDFLAGS=-g -L$(HOME)/lib -ldb_cxx -lindri -lz -lpthread -lm
endif

CXXFLAGS= -DP_NEEDS_GNU_CXX_NAMESPACE=1 -DNDEBUG=1 -DNDEBUG  -g $(INCPATH)
ifdef DIST
	CXXFLAGS+=-O3	
endif 

COMMON_OBJECTS=$(COMMON_SOURCES:.cpp=.o)
TAILY_OBJECTS=$(TAILY_SOURCES:.cpp=.o)
INDRI_OBJECTS=$(INDRI_SOURCES:.cpp=.o)
DOCDUMP_OBJECTS=$(DOCDUMP_SOURCES:.cpp=.o)

all: taily indri docdump

taily: $(TAILY_TARGET)

indri: $(INDRI_TARGET)

docdump: $(DOCDUMP_TARGET)

$(TAILY_TARGET): $(TAILY_OBJECTS) $(COMMON_OBJECTS)
	$(CXX) -o $@ $^ $(LDFLAGS)
	
$(INDRI_TARGET): $(INDRI_OBJECTS) $(COMMON_OBJECTS)
	$(CXX) -o $@ $^ $(LDFLAGS)
	
$(DOCDUMP_TARGET): $(DOCDUMP_OBJECTS) $(COMMON_OBJECTS)
	$(CXX) -o $@ $^ $(LDFLAGS)

%.o: %.cpp %.h
	$(CXX) $(CXXFLAGS) -o $@ -c $<

clean:
	rm -f $(TAILY_TARGET) $(INDRI_TARGET) $(DOCDUMP_TARGET) $(COMMON_OBJECTS) $(TAILY_OBJECTS) $(INDRI_OBJECTS) $(DOCDUMP_OBJECTS)

