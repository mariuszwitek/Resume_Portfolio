#include <stdio.h>
#include <iostream>
#include <string.h>
#include <string>
#include <vector>
#include <algorithm>
#include <mysql.h>

class RawHitStorage
{
public:
    RawHitStorage();
    ~RawHitStorage();

    // TDC Values
    unsigned int tdcSpillID[20000000];
    unsigned int tdcEventID[20000000];
    unsigned char tdcROC[20000000];
    unsigned short int tdcBoardID[20000000];
    unsigned char tdcChannelID[20000000];
    double tdcStopTime[20000000];

    // v1495 Values
    unsigned int v1495SpillID[20000000];
    unsigned int v1495EventID[20000000];
    unsigned char v1495ROC[20000000]; 
    unsigned short int v1495BoardID[20000000];
    unsigned char v1495ChannelID[20000000];
    double v1495StopTime[20000000];
   
    unsigned char curROCID;

    // Keep count of how many are in each sets of arrays
    int tdcCount;
    int v1495Count;
    
    // Count of all hits/triggerhits
    int hit_count;
    int thit_count;
   
    // Tracking 1495 bugs
    int all1495, good1495, d1ad1495, d2ad1495, d3ad1495;

    void insert_hit(unsigned int spillID, unsigned int eventID, unsigned char rocID,
                    unsigned short int boardID, unsigned char channelID,
                    double tdcTime);
    void insert_trigger_hit(unsigned int spillID, unsigned int eventID, 
                            unsigned char rocID, unsigned short int boardID,
                            unsigned char channelID, double tdcTime);

    std::vector<int> get_events();

    void clear();
    void clear_hit();
    void clear_trigger_hit();
    
private:
    int max_entries;
    std::vector<int> events_from_hit();
    std::vector<int> events_from_trigger_hit();
};

class RawScalerStorage
{

private:
    int max_entries;

public:
    RawScalerStorage();
    ~RawScalerStorage();
   
    unsigned char spillType;
    unsigned char curROCID;

    unsigned int scalerSpillID[1024];
    unsigned char scalerSpillType[1024];
    unsigned int scalerCodaEventID[1024];
    unsigned char scalerROC[1024];
    unsigned short int scalerBoardID[1024];
    unsigned char scalerChannelID[1024];
    int scalerValue[1024];
    
    int scalerCount;
    
    void insert_scaler(unsigned int spillID, unsigned int codaEventID, unsigned char type,
                       unsigned char rocID, unsigned short int boardID,
                       unsigned char channelID, int value);
    void print_scalers();
    void clear_scaler();
};

class Event
{
public:
    Event();
    ~Event();

    int eventID;
    int codaEventID;
    int runID;
    int spillID;
    int dataQuality;
    int vmeTime;
    int RawMATRIX[5];
    int AfterInhMATRIX[5];
    int NIM[5];
    int MATRIX[5];

    int qieFlag;
    unsigned int triggerCount;
    unsigned int turnOnset;
    unsigned int rfOnset;
    unsigned int rf[33];

    std::string event_to_str();
    std::string qie_to_str();

};

class RawEventStorage
{
public:

    RawEventStorage();
    ~RawEventStorage();
    
    int eventTotal;
    int eventCount;

    void insert_event(Event* e);
    int submit(MYSQL* conn);
    void clear();

private:

    int submit_events(MYSQL* conn);
    int submit_qie(MYSQL* conn);
    std::vector<Event*> v_event;

};

