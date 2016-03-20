#include "RawStorage.h"

RawHitStorage::RawHitStorage()
{
    clear();
    max_entries = 20000000;
    tdcCount = 0;
    v1495Count = 0;
    hit_count = 0;
    thit_count = 0;
    all1495 = 0;
    good1495 = 0;
    d1ad1495 = 0;
    d2ad1495 = 0;
    d3ad1495 = 0;
}

RawHitStorage::~RawHitStorage()
{
    clear();
}

void RawHitStorage::clear()
{
    clear_hit();
    clear_trigger_hit();
}

void RawHitStorage::clear_hit()
{
    memset ( (void *) &tdcSpillID, 0, sizeof (unsigned int) *max_entries);
    memset ( (void *) &tdcEventID, 0, sizeof (unsigned int) *max_entries);
    memset ( (void *) &tdcROC, 0, sizeof (unsigned char) *max_entries);
    memset ( (void *) &tdcBoardID, 0, sizeof (unsigned short int) *max_entries);
    memset ( (void *) &tdcChannelID, 0, sizeof (unsigned char) *max_entries);
    memset ( (void *) &tdcStopTime, 0, sizeof (double) *max_entries);
    tdcCount = 0;
}

void RawHitStorage::clear_trigger_hit()
{
    memset ( (void *) &v1495SpillID, 0, sizeof (unsigned int) *max_entries);
    memset ( (void *) &v1495EventID, 0, sizeof (unsigned int) *max_entries);
    memset ( (void *) &v1495ROC, 0, sizeof (unsigned char) *max_entries);
    memset ( (void *) &v1495BoardID, 0, sizeof (unsigned short int) *max_entries);
    memset ( (void *) &v1495ChannelID, 0, sizeof (unsigned char) *max_entries);
    memset ( (void *) &v1495StopTime, 0, sizeof (double) *max_entries);
    v1495Count = 0;
}

void RawHitStorage::insert_hit(unsigned int spillID, unsigned int eventID,
                          unsigned char rocID, unsigned short int boardID,
                          unsigned char channelID, double tdcTime)
{
    tdcEventID[tdcCount] = eventID;
    tdcSpillID[tdcCount] = spillID;
    tdcROC[tdcCount] = rocID;
    tdcBoardID[tdcCount] = boardID;
    tdcChannelID[tdcCount] = channelID;
    tdcStopTime[tdcCount] = tdcTime;
    tdcCount++;
    hit_count++;

    if (tdcCount == max_entries)
    {   
        printf("Too many TDC Entries!\n");
        clear_hit();
    }
}

void RawHitStorage::insert_trigger_hit(
        unsigned int spillID, unsigned int eventID, unsigned char rocID, 
        unsigned short int boardID, unsigned char channelID, double tdcTime)
{
    v1495EventID[v1495Count] = eventID; 
    v1495SpillID[v1495Count] = spillID; 
    v1495ROC[v1495Count] = rocID; 
    v1495BoardID[v1495Count] = boardID; 
    v1495ChannelID[v1495Count] = channelID; 
    v1495StopTime[v1495Count] = tdcTime; 
    v1495Count++; 
    thit_count++;

    if (v1495Count == max_entries) 
    {
        printf("Too many v1495 Entries!\n");
        clear_trigger_hit();
    }
}

std::vector<int> RawHitStorage::events_from_hit()
{
    std::vector<int> events;
    unsigned int eventID;
    if (tdcCount > 0)
    {
        eventID = tdcEventID[0];
        events.push_back(eventID);
        for (int i = 0 ; i < tdcCount ; i++)
        {
            if (tdcEventID[i] != eventID)
            {
                eventID = tdcEventID[i];
                events.push_back(eventID);
            }
        }
    } 

    return events;
}

std::vector<int> RawHitStorage::events_from_trigger_hit()
{
    std::vector<int> events;
    unsigned int eventID;
    if (v1495Count > 0)
    {
        eventID = v1495EventID[0];
        events.push_back(eventID);
        for (int i = 0 ; i < v1495Count ; i++)
        {
            if (v1495EventID[i] != eventID)
            {
                eventID = v1495EventID[i];
                events.push_back(eventID);
            }
        }
    } 

    return events;
}

std::vector<int> RawHitStorage::get_events()
{
    std::vector<int> events1;
    std::vector<int> events2;

    events1 = events_from_hit();
    events2 = events_from_trigger_hit();

    int maxsize = events1.size() + events2.size();
    
    std::vector<int> events_all(maxsize);
    std::vector<int>::iterator it;

    std::sort(events1.begin(), events1.begin()+events1.size());
    std::sort(events2.begin(), events2.begin()+events2.size());
 
    it=std::set_union (events1.begin(), events1.begin()+events1.size(),
                  events2.begin(), events2.begin()+events2.size(), 
                  events_all.begin());
    events_all.resize(it-events_all.begin());

    return events_all;
    
}

RawScalerStorage::RawScalerStorage()
{
    clear_scaler();
    spillType = 1; 
    max_entries = 1024;
    scalerCount = 0;
}

RawScalerStorage::~RawScalerStorage()
{
    clear_scaler();
}

void RawScalerStorage::clear_scaler()
{
    memset ( (void *) &scalerSpillID, 0, sizeof (unsigned int) *max_entries);
    memset ( (void *) &scalerSpillType, 0, sizeof (unsigned char) *max_entries);
    memset ( (void *) &scalerCodaEventID, 0, sizeof (unsigned int) *max_entries);
    memset ( (void *) &scalerROC, 0, sizeof (unsigned char) *max_entries);
    memset ( (void *) &scalerBoardID, 0, sizeof (unsigned short int) *max_entries);
    memset ( (void *) &scalerChannelID, 0, sizeof (unsigned char) *max_entries);
    memset ( (void *) &scalerValue, 0, sizeof (int) *max_entries);
    scalerCount = 0;
}

void RawScalerStorage::insert_scaler(
        unsigned int spillID, unsigned int codaEventID, unsigned char type,
        unsigned char rocID, unsigned short int boardID,
        unsigned char channelID, int value)
{
    scalerSpillID[scalerCount] = spillID;
    scalerSpillType[scalerCount] = type;
    scalerCodaEventID[scalerCount] = codaEventID;
    scalerROC[scalerCount] = rocID;
    scalerBoardID[scalerCount] = boardID;
    scalerChannelID[scalerCount] = channelID;
    scalerValue[scalerCount] = value;
    scalerCount++;

    if (scalerCount == max_entries)
    {   
        printf("Too many Scaler Entries!\n");
        clear_scaler();
    }
}

void RawScalerStorage::print_scalers()
{
    int i;
    for (i=0;i<scalerCount;i++)
    {
        printf("%i\t%i\t%i\t%i\t%i\t%i\n",
               scalerSpillID[i], scalerCodaEventID[i], scalerROC[i],
               scalerBoardID[i], scalerChannelID[i], scalerValue[i]);
    }

}

Event::Event()
{
    dataQuality = 0;
    for (int i=0;i<33;i++){
        rf[i]=0;
    }

    qieFlag = 0;
    triggerCount = 0;
    turnOnset = 0;
    rfOnset = 0;
}

Event::~Event()
{
}

std::string Event::event_to_str()
{
    std::string ss;
    ss = (std::to_string(static_cast<unsigned long long>(eventID)) + ", " +
          std::to_string(static_cast<unsigned long long>(codaEventID)) + ", " +
          std::to_string(static_cast<unsigned long long>(runID)) + ", " +
          std::to_string(static_cast<unsigned long long>(spillID)) + ", " +
          std::to_string(static_cast<unsigned long long>(dataQuality)) + ", " +
          std::to_string(static_cast<unsigned long long>(vmeTime)) + ", " +
          std::to_string(static_cast<unsigned long long>(RawMATRIX[0])) + ", " +
          std::to_string(static_cast<unsigned long long>(RawMATRIX[1])) + ", " +
          std::to_string(static_cast<unsigned long long>(RawMATRIX[2])) + ", " +
          std::to_string(static_cast<unsigned long long>(RawMATRIX[3])) + ", " +
          std::to_string(static_cast<unsigned long long>(RawMATRIX[4])) + ", " +
          std::to_string(static_cast<unsigned long long>(AfterInhMATRIX[0])) + ", " +
          std::to_string(static_cast<unsigned long long>(AfterInhMATRIX[1])) + ", " +
          std::to_string(static_cast<unsigned long long>(AfterInhMATRIX[2])) + ", " +
          std::to_string(static_cast<unsigned long long>(AfterInhMATRIX[3])) + ", " +
          std::to_string(static_cast<unsigned long long>(AfterInhMATRIX[4])) + ", " +
          std::to_string(static_cast<unsigned long long>(NIM[0])) + ", " +
          std::to_string(static_cast<unsigned long long>(NIM[1])) + ", " +
          std::to_string(static_cast<unsigned long long>(NIM[2])) + ", " +
          std::to_string(static_cast<unsigned long long>(NIM[3])) + ", " +
          std::to_string(static_cast<unsigned long long>(NIM[4])) + ", " +
          std::to_string(static_cast<unsigned long long>(MATRIX[0])) + ", " +
          std::to_string(static_cast<unsigned long long>(MATRIX[1])) + ", " +
          std::to_string(static_cast<unsigned long long>(MATRIX[2])) + ", " +
          std::to_string(static_cast<unsigned long long>(MATRIX[3])) + ", " +
          std::to_string(static_cast<unsigned long long>(MATRIX[4])));

    return ss;
}

std::string Event::qie_to_str()
{
    std::string ss;
    //std::cout << eventID << turnOnset << std::endl;
    ss = (std::to_string(static_cast<unsigned long long>(runID)) + ", " +
          std::to_string(static_cast<unsigned long long>(spillID)) + ", " +
          std::to_string(static_cast<unsigned long long>(eventID)) + ", " +
          std::to_string(static_cast<unsigned long long>(triggerCount)) + ", " +
          std::to_string(static_cast<unsigned long long>(turnOnset)) + ", " +
          std::to_string(static_cast<unsigned long long>(rfOnset)) + ", ");
    for (int i = 0 ; i < 33 ; i++)
    {
          ss = ss + std::to_string(static_cast<unsigned long long>(rf[i])) + ", ";
    };

    ss = ss.substr(0,ss.size()-2);
    return ss;
}

RawEventStorage::RawEventStorage()
{
    eventTotal = 0;
    eventCount = 0;
    clear();
}

RawEventStorage::~RawEventStorage()
{
    clear();
}

void RawEventStorage::clear()
{
    v_event.clear();
    eventCount = 0;
}

void RawEventStorage::insert_event(Event* e)
{
    v_event.push_back(e);
    eventTotal++;
    eventCount++;
}

int RawEventStorage::submit(MYSQL* conn)
{
    int val = 0;
    if (eventCount > 0)
    {
        val = submit_events(conn);
        val = submit_qie(conn);
    }
    clear();

    return val;
}

int RawEventStorage::submit_events(MYSQL* conn)
{
    std::string query = ("INSERT INTO Event (eventID, codaEventID, runID, spillID, "
            "dataQuality, vmeTime, RawMATRIX1, RawMATRIX2, RawMATRIX3, RawMATRIX4, "
            "RawMATRIX5, AfterInhMATRIX1, AfterInhMATRIX2, AfterInhMATRIX3, AfterInhMATRIX4, "   
            "AfterInhMATRIX5,NIM1,NIM2,NIM3,NIM4,NIM5,MATRIX1,MATRIX2,MATRIX3,MATRIX4,MATRIX5) " 
            "VALUES ");
    for (auto it = v_event.begin(); it != v_event.end(); ++it)
    {
        query += "(" + (*it)->event_to_str() + "), ";
    }
    query = query.substr(0, query.size()-2);

    if ( mysql_query(conn, query.c_str()) )
    {
        std::cout << "An Error has occurred while inserting Events: " << mysql_error (conn) 
                  << std::endl;
        std::cout << query << std::endl;
        return 1;
    }

    return 0;
}

int RawEventStorage::submit_qie(MYSQL* conn)
{
    int submit = 0;
    std::string query = (
            "INSERT INTO QIE (`runID`, `spillID`, `eventID`, `triggerCount`, "
            "`turnOnset`, `rfOnset`, "
            "`RF-16`, `RF-15`, `RF-14`, `RF-13`, `RF-12`, `RF-11`, `RF-10`, "
            "`RF-09`, `RF-08`, `RF-07`, `RF-06`, `RF-05`, `RF-04`, `RF-03`, "
            "`RF-02`, `RF-01`, `RF+00`, `RF+01`, `RF+02`, `RF+03`, `RF+04`, "
            "`RF+05`, `RF+06`, `RF+07`, `RF+08`, `RF+09`, `RF+10`, `RF+11`, "
            "`RF+12`, `RF+13`, `RF+14`, `RF+15`, `RF+16`) VALUES ");
            
    for (auto it = v_event.begin(); it != v_event.end(); ++it)
    {
        if ((*it)->qieFlag == 1)
        {
            query += "(" + (*it)->qie_to_str() + "), ";
            submit = 1;
        }
    }
    query = query.substr(0, query.size()-2);
    //std::cout << query << std::endl;

    if (submit)
    {
        if ( mysql_query(conn, query.c_str()) )
        {
            std::cout << "An Error has occurred while inserting QIE Entries: " << mysql_error (conn) 
                      << std::endl;
            std::cout << query << std::endl;
            return 1;
        }
    }
    return 0;
}
