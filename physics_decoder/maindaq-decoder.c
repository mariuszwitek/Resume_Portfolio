//
//	CODA to MySQL Decoder
//	Creator: Bryan Dannowitz
//
//	Indended for Use at the SeaQuest Experiment @ FNAL
//
//////////////////////////////////////////////////////////////// 

#include "maindaq-decoder.h"

int main (int argc, char *argv[])
{
    // This is the array that holds CODA event information
    int physicsEvent[70000];
    
    // MySQL variables
    MYSQL *conn;
    MYSQL_RES *res;
    const char user[] = "XXXXXXXXXX";
    const char pass[] = "XXXXXXXXXXXX";
    int PORT;
    char *UNIX_SOCKET = NULL;
    const int CLIENT_FLAG = 0;
    
    // File handling variables
    FILE *fp;
    int reconnect = 1;
    long int fileSize;
    int stopped = 0;
    int sleepCounter = 0;
    char end_file_name [1024];
    char next_run_name [1024];
    
    // Some other variables used
    int i;
    int physEvCount = 0;
    int status;
    int eventIdentifier;
    
    // Initialize some important values
    timeTotal 	 = 0.0;
    hitID        = 0;
    codaEventID  = 0;
    eventID 	 = 0;
    spillID 	 = 0;
    totalTriggerBits = 0;
    sampling_count = 1;
    
    DocoptArgs args = docopt(argc, argv, /* help */ 1, /* version */ "2.0rc2");
    verbose = args.verbose;

    strcpy(end_file_name, "");
    strcpy(next_run_name, "");

    // Initialize MySQL database connection
    conn = mysql_init (NULL);
    
    // Must set this option for the C API to allow loading data from
    // 	local file (like we do for slow control events)
    mysql_options (conn, MYSQL_OPT_LOCAL_INFILE, NULL);
    mysql_options (conn, MYSQL_OPT_RECONNECT, &reconnect);

    if (!strcmp(args.server,"XXXXXXXXXXXXXXXXXXXXXXXXXXXX"))
    { PORT = XXXX; } else { PORT = XXXX; }

    // Connect to the MySQL database
    if (!mysql_real_connect (conn, args.server, user, pass, args.schema,
                             PORT, UNIX_SOCKET, CLIENT_FLAG) )
    {
        fprintf (stderr, "Database Connection ERROR: %s\n\n", mysql_error (conn) );
        return 1;
    }
    else
    {
        fprintf (stdout, "Database connection: Success\n\n");
    }

    // Create database and tables if they are not currently there.
    if (createSQL (conn, args.schema, args.online) )
    {
        fprintf (stderr, "SQL Table Creation ERROR\n");
        return 1;
    }

    memset ( (void *) &physicsEvent, 0, sizeof (int) * 70000);

    // Check if specified file exists
    if (file_exists (args.file) )
    {
        // Get file size
        fp = fopen (args.file, "r");
        fseek (fp, 0L, SEEK_END);
        fileSize = ftell (fp);

        // evOpen will return an error if the file is less than
        //	a certain size, so wait until the file is big enough.
        while (fileSize < 32768 && sleepCounter < 40)
        {
            sleep (15);
            // Get file size again
            if (verbose) 
            {
                fprintf (stdout, "File too small. Waiting for more...\n");
            }
            fseek (fp, 0L, SEEK_END);
            fileSize = ftell (fp);
            sleepCounter++;
        }

        if (sleepCounter == 40)
        {
            fprintf (stderr, "File not large enough; Wait timeout. Exiting...\n\n");
            return 1;
        }

        if (verbose)
        {
            fprintf (stdout, "Loading... \"%s\"\n", args.file);
        }
        fclose (fp);
        status = open_coda_file (args.file);

        if (status == SUCCESS)
        {
            if (verbose)
            {
                fprintf (stdout, "Opening CODA File Status: OK!\n\n");
            }
        }
        else
        {
            fprintf (stderr, "Problem Opening CODA File\nStatus Code: %x\n\nExiting!\n\n", status);
            return 1;
        }

        // If the file does NOT exist:
    }
    else
    {
        fprintf (stderr, "File \"%s\" does not exist.\nExiting...\n", args.file);
        return 1;
    }

    // ROOT FILE HANDLING:
    // ------------------
    // Populate the SCODAEvent structure
    // then saveTree->Fill()
    // at end of decoding, close the file
    saveFile = new TFile(args.root, "RECREATE");
    saveTree = new TTree("save", "save");

    // initialize the empty event structre
    rawEvent = new SRawEvent();
    
    // the args are: branch name, address of the pointer to a ROOT class,
    saveTree->Branch("rawEvent", &rawEvent, 256000, 99); 

    rawEventBuffer.clear();
    // ------------------
    
    // Raw Event, Hit, and Scaler Container Initialization
    RawEventStorage* eventStorage = new RawEventStorage();
    RawHitStorage* hitStorage = new RawHitStorage();
    RawScalerStorage* scalerStorage = new RawScalerStorage();
    
    timeStart = time (NULL);
    
    // Read the first event
    spillID = 0;
    status = read_coda_file (physicsEvent, 70000);
    codaEventID = 1;
    eventIdentifier = physicsEvent[1];

    for (i = 0; eventIdentifier != EXIT_CASE; i++)
    {
        // If an error is hit while decoding an offline file, something's wrong
        while (status != SUCCESS)
        {
            if (status!=-1) fprintf(stdout, "read_coda_file() returns: %i\n"
                                    "Searching for END file. "
                                    "If not found, will wait %i seconds "
                                    "for more events.\n",status,WAIT_TIME);

            if (file_exists (end_file_name) || file_exists (next_run_name) || args.grid || !args.online)
            {
                wrapup (conn, args, eventStorage, hitStorage);
                return 0;
            }
            else
            {
                // If an error is hit while decoding an online file, it likely just means
                // 	you need to wait for more events to be written.  Wait and try again.

                // Get file size the first time through this while loop
                if ( stopped == 0 )
                {
                    fp = fopen (args.file, "r");
                    fseek (fp, 0L, SEEK_END);
                    fileSize = ftell (fp);
                    fclose (fp);
                    stopped = 1;
                    res = mysql_store_result (conn);
                    mysql_free_result (res);
                }

                // Wait, then see if the file has grown by at least one block size
                status = retry (args.file, fileSize, i, physicsEvent);

                if ( status == SUCCESS )
                {
                    timeStart = time (NULL);
                    stopped = 0;
                }
            }
        }

        // The last 4 hex digits will be 0x01cc for Prestart, Go Event,
        //		and End Event, and 0x10cc for Physics Events
        switch (eventIdentifier & 0xFFFF)
        {
        case PHYSICS_EVENT:
            if ( (codaEventID % 1000 == 0) && (codaEventID != 0) )
            {
                if (verbose) printf ("%i Events\n", codaEventID);
                if ( submit_all_data(conn, args, eventStorage, hitStorage) )
                {
                    fprintf (stderr, "Error submitting data...\n");
                    return 1;
                }
            }

            if (get_hex_bits (physicsEvent[1], 7, 4) == 140)
            {
                // Start of run descriptor
                if ( eventRunDescriptorSQL (conn, physicsEvent) )
                {
                    fprintf (stderr, "Run Descriptor Event Processing Error. Exiting...\n\n");
                    return 1;
                }

                break;
            }
            else
            {
                if (eventSQL (conn, physicsEvent, args, eventStorage, scalerStorage, hitStorage) )
                {
                    return 1;
                }

                physEvCount++;
                break;
            }

        case CODA_EVENT:
            switch (eventIdentifier)
            {
            case PRESTART_EVENT:

                if ( prestartSQL (conn, physicsEvent, args) )
                {
                    fprintf(stderr, "Error while handling Prestart Event. Exiting...\n\n");
                    return 1;
                }
                if (!args.grid) sprintf (end_file_name, "/data2/e906daq/coda/data/END/%i.end", runNum);
                if (!args.grid) sprintf (next_run_name, "/codadata/run_%.6i.dat", (runNum + 1) );
                if (!args.online)
                {
                    if ( mysql_query (conn, "ALTER TABLE Hit DISABLE KEYS") )
                    {
                        fprintf (stderr, "Hit Table Disable Keys Error: %s\n", mysql_error (conn) );
                        return 1;
                    }

                    if ( mysql_query (conn, "ALTER TABLE TriggerHit DISABLE KEYS") )
                    {
                        fprintf (stderr, "TriggerHit Table Disable Keys Error: %s\n", mysql_error (conn) );
                        return 1;
                    }

                }
                break;

            case GO_EVENT:
                goEventSQL (physicsEvent);
                break;

            case END_EVENT:
                if ( endEventSQL (physicsEvent) )
                {
                    return 1;
                }

                eventIdentifier = EXIT_CASE;
                if (verbose) printf ("End Event Processed\n");
                break;

            default:
                fprintf (stderr, "Uncovered Case: %x\n", eventIdentifier);
                return 1;
                break;
            }

            break;

        case FEE_PREFIX:
            if (eventIdentifier == FEE_EVENT)
            {
                // Process only if there is content
                if (physicsEvent[0] > 8)
                {
                    //for(x=0;x<20;x++) printf("physicsEvent[%i] = %.8x\n",x,physicsEvent[x]);
                    if (physicsEvent[3] == (int)0xe906f011)
                    {
                        if ( feeEventSQL (conn, physicsEvent) )
                        {
                            return 1;
                        }
                    }
                    else if (physicsEvent[3] == (int)0xe906f012)
                    {
                        if ( feePrescaleSQL (conn, physicsEvent) )
                        {
                            return 1;
                        }
                    }
                }
            }
            else
            {
                fprintf (stderr, "Uncovered Case: %x\n", eventIdentifier);
                return 1;
            }

            break;

        case 0:
            // Special case which requires waiting and retrying
            status = retry (args.file, 0, (i - 1), physicsEvent);
            i--;
            break;

        default:
            // If no match to any given case, print it and exit.
            fprintf (stderr, "Uncovered Case: %x\n", eventIdentifier);
            return 1;
            break;
        }

        // Clear the buffer, read the next event, and go back through the loop.
        if (eventIdentifier != EXIT_CASE)
        {
            memset ( (void *) &physicsEvent, 0, sizeof (int) * 70000);
            status = read_coda_file (physicsEvent, 70000);
            codaEventID++;
            eventIdentifier = physicsEvent[1];
        }
    }

    wrapup (conn, args, eventStorage, hitStorage);
    return 0;
};


int retry (char *file, long int oldfilesize, int codaEventCount, int physicsEvent[70000])
{
    // ================================================================
    //
    // This function is used in the case that a file is being decoded
    //	while it's still being written.  When/if an EOF is reached
    //	before an End Event is processed, evRead will throw an error status.
    //	This function handles this problem, which
    //	closes the CODA file, waits a couple seconds (specified by the
    //	the WAIT_TIME constant in the header), opens the file,
    //	and loops evRead enough times to get to the desired event.
    //
    // Returns:	the status of the last evRead
    //		0 (SUCCESS) on success
    //		!0 for a myriad of other errors, including
    //		-1 (EOF) (ERROR) on error
    int status, k;
    // File handling variables
    FILE *fp;
    long int newfilesize;
    // Wait N seconds
    sleep (WAIT_TIME);
    printf (".");
    // Get more recent file size
    fp = fopen (file, "r");
    fseek (fp, 0L, SEEK_END);
    newfilesize = ftell (fp);

    // If the file grows by at least a single block size, get the next new event
    if ( newfilesize > (oldfilesize + 32768) )
    {
        // Open up the CODA file again
        status = open_coda_file (file);

        if (status != SUCCESS)
        {
            return status;
        }

        // If closed and opened successfully, loop through to the desired event.
        codaEventID = 0;

        for (k = 0 ; k <= codaEventCount ; k++)
        {
            status = read_coda_file (physicsEvent, 70000);
            codaEventID++;
        }
    }
    else
    {
        status = ERROR;
    }

    fclose (fp);
    return status;
}

int wrapup (MYSQL *conn, DocoptArgs args, RawEventStorage* eventStorage,
            RawHitStorage* hitStorage)
{
    MYSQL_RES *res;
    MYSQL_ROW row;
    char qryString[1000];
    char intensity_field[64];
    char beamdaqfile[1024];
    unsigned int targbits;
    unsigned int target[7];
    int rowcount, found;
    unsigned long *lengths;
    int PORT;

    if (!strcmp(args.server,"seaquel.physics.illinois.edu"))
    { PORT = 3283; } else { PORT = 3306; }

    if ( submit_all_data(conn, args, eventStorage, hitStorage) )
    {
        fprintf (stderr, "Error submitting data...\n");
        return 1;
    }

    // ROOT FILE HANDLING
    saveTree->AutoSave("SaveSelf");
    saveTree->Write();
    saveFile->cd();
    saveFile->Close();
    // ------------------

    memset ( (void *) &target, 0, sizeof (unsigned int) * 7);
    targbits = 0;
  
    mysql_query(conn, "DROP TABLE IF EXISTS SlowControl");
    mysql_query(conn, "DROP TABLE IF EXISTS prestartInfo");

    // MAGNET SUMMARY -----------------------------------------------
    sprintf (qryString, "SELECT `fmag-avg`, `kmag-avg` FROM "
             "( SELECT AVG(CAST(value AS DECIMAL(7,3))) AS `fmag-avg` "
             "FROM Beam WHERE name='F:NM3S' AND runID=%i) t1, "
             "( SELECT AVG(CAST(value AS DECIMAL(7,3))) AS `kmag-avg` "
             "FROM Beam WHERE name='F:NM4AN' AND runID=%i) t2",
             runNum, runNum);

    if ( mysql_query (conn, qryString) )
    {
        fprintf (stderr, "Magnet Summary Error: %s\n", mysql_error (conn) );
        return 1;
    }


    res = mysql_store_result (conn);
    row = mysql_fetch_row (res);
    lengths = mysql_fetch_lengths (res);

    if (lengths[0] > 0 && lengths[1] > 0)
    {
        sprintf (qryString, "UPDATE summary.Run2Summary SET `FMAG-Avg`=%f, `KMAG-Avg`=%f WHERE runID=%i",
                 atof (row[0]), atof (row[1]), runNum);

        if ( mysql_query (conn, qryString) )
        {
            fprintf (stderr, "Run2Summary Magnet Update Error (Ignoring Error): %s\n", mysql_error (conn) );
        }

        sprintf (qryString, "INSERT INTO Run VALUES (%i, \"FMAG-Avg\", %.3f), (%i, \"KMAG-Avg\", %.3f)",
                 runNum, atof (row[0]), runNum, atof (row[1]) );

        if ( mysql_query (conn, qryString) )
        {
            fprintf (stderr, "Magnet Update Error: %s\n", mysql_error (conn) );
            return 1;
        }
    }

    mysql_free_result (res);
    
    // END MAGNET SUMMARY -----------------------------------------------
    
    // TARGET POSITION SUMMARY -----------------------------------------------
    
    sprintf (qryString, "SELECT DISTINCT targetPos FROM Spill WHERE runID=%i", runNum);

    if ( mysql_query (conn, qryString) )
    {
        fprintf (stderr, "TargetPosition Summary Error: %s\n", mysql_error (conn) );
        return 1;
    }

    res = mysql_store_result (conn);

    while ((row = mysql_fetch_row (res)) )
    {
        targbits += pow (2, atoi (row[0]) );
        target[atoi (row[0]) - 1] = 1;
    }

    mysql_free_result (res);

    sprintf (qryString, "UPDATE summary.Run2Summary SET LH2=%i, LD2=%i, Solid1=%i, "
             "Solid2=%i, Solid3=%i, Solid4=%i, Empty=%i WHERE runID=%i",
             target[0], target[2], target[3], target[4], target[5],
             target[6], target[1],  runNum);

    if ( mysql_query (conn, qryString) )
    {
        fprintf (stderr, "Run2Summary TargetPosition Update Error (Ignoring Error): %s\n", mysql_error (conn) );
    }

    sprintf (qryString, "INSERT INTO Run VALUES "
             "(%i, \"TargetLH2\", %i),"
             "(%i, \"TargetLD2\", %i),"
             "(%i, \"TargetSolid1\", %i),"
             "(%i, \"TargetSolid2\", %i),"
             "(%i, \"TargetSolid3\", %i),"
             "(%i, \"TargetSolid4\", %i),"
             "(%i, \"TargetEmpty\", %i)",
             runNum, target[0], runNum, target[2], runNum, target[3], runNum, target[4],
             runNum, target[5], runNum, target[6], runNum, target[1]);

    if ( mysql_query (conn, qryString) )
    {
        fprintf (stderr, "TargetPosition, TriggerBits Update Error: %s\n", mysql_error (conn) );
        return 1;
    }

    // END TARGET POSITION SUMMARY -----------------------------------------------
    
    // TRIGGER SUMMARY -----------------------------------------------
    
    if ( mysql_query (conn, "SELECT 1 FROM Event") )
    {
        fprintf (stderr, "Event Count Check Error: %s\n", mysql_error (conn) );
        return 1;
    }
    res = mysql_store_result (conn);
    rowcount = mysql_num_rows(res);
    mysql_free_result (res);

    if (rowcount > 0)
    {
        if ( mysql_query (conn, "SELECT SUM(NIM1), SUM(NIM2), SUM(NIM3), SUM(NIM4), SUM(NIM5), "
                          "SUM(MATRIX1), SUM(MATRIX2), SUM(MATRIX3), SUM(MATRIX4), SUM(MATRIX5) FROM Event") )
        {
            fprintf (stderr, "TriggerEvent Run Summary Error: %s\n", mysql_error (conn) );
            return 1;
        }

        res = mysql_store_result (conn);

        row = mysql_fetch_row (res);
        sprintf (qryString, "UPDATE summary.Run2Summary SET NIM1Events=%i, NIM2Events=%i, NIM3Events=%i, "
                 "NIM4Events=%i, NIM5Events=%i, MATRIX1Events=%i, MATRIX2Events=%i, MATRIX3Events=%i, "
                 "MATRIX4Events=%i, MATRIX5Events=%i WHERE runID=%i",
                 atoi (row[0]), atoi (row[1]), atoi (row[2]), atoi (row[3]), atoi (row[4]), atoi (row[5]),
                 atoi (row[6]), atoi (row[7]), atoi (row[8]), atoi (row[9]), runNum);

        if ( mysql_query (conn, qryString) )
        {
            fprintf (stderr, "Update TriggerEvents Run Summary Error (Ignoring Error): %s\n", mysql_error (conn) );
        }

        sprintf (qryString, "INSERT INTO Run VALUES "
                 "(%i, \"NIM1Events\", %s),"
                 "(%i, \"NIM2Events\", %s),"
                 "(%i, \"NIM3Events\", %s),"
                 "(%i, \"NIM4Events\", %s),"
                 "(%i, \"NIM5Events\", %s),"
                 "(%i, \"MATRIX1Events\", %s),"
                 "(%i, \"MATRIX2Events\", %s),"
                 "(%i, \"MATRIX3Events\", %s),"
                 "(%i, \"MATRIX4Events\", %s),"
                 "(%i, \"MATRIX5Events\", %s)",
                 runNum, row[0], runNum, row[1], runNum, row[2], runNum, row[3], runNum, row[4],
                 runNum, row[5], runNum, row[6], runNum, row[7], runNum, row[8], runNum, row[9]);
        if ( mysql_query (conn, qryString) )
        {
            fprintf (stderr, "Update TriggerEvents Run Summary Error: %s\n", mysql_error (conn) );
            return 1;
        }
        
        mysql_free_result (res);
    } 
    // END TRIGGER SUMMARY -----------------------------------------------

    // BEAMDAQ FILE UPLOAD -----------------------------------------------
    
    if (args.grid)
    {
        sprintf(beamdaqfile,"%s/db_run%i.dat", getenv("CONDOR_DIR_INPUT"), runNum);
    }
    else
    {
        sprintf(beamdaqfile,"/data2/data/beamDAQ/cerenkov_data/db/db_run%i.dat",runNum);
    }

    if (file_exists(beamdaqfile))
    {
        sprintf (qryString, "LOAD DATA LOCAL INFILE '%s' "
                 "INTO TABLE BeamDAQ "
                 "FIELDS TERMINATED BY ' ' "
                 "LINES TERMINATED BY '-9999 \\n' "
                 "IGNORE 1 LINES", beamdaqfile);

        // Submit the query to the server
        if (mysql_query (conn, qryString) )
        {
            fprintf (stderr, "%s\n", qryString);
            fprintf (stderr, "BeamDAQ Load Error: %s\n", mysql_error (conn) );
            return 1;
        }
    }
    
    // END BEAMDAQ FILE UPLOAD -----------------------------------------------

    // LIVE PROTON CALCULATION -----------------------------------------------
    found = 0;

    if (mysql_query (conn, "SELECT 1 FROM Beam WHERE name='S:G2SEM' AND value>0 LIMIT 1"))
    {
        fprintf (stderr, "%s\n", qryString);
        fprintf (stderr, "S:G2SEM Check Error: %s\n", mysql_error (conn) );
        return 1;
    }
    res = mysql_store_result(conn);
    rowcount = mysql_num_rows(res);
    mysql_free_result(res);
    if (rowcount == 0)
    {
        if (mysql_query (conn, "SELECT 1 FROM Beam WHERE name='F:NM3SEM' AND value>0 LIMIT 1"))
        {
            fprintf (stderr, "%s\n", qryString);
            fprintf (stderr, "F:NM3SEM Check Error: %s\n", mysql_error (conn) );
            return 1;
        }
        res = mysql_store_result(conn);
        rowcount = mysql_num_rows(res);
        mysql_free_result(res);
    
        if (rowcount == 0)
        {
            if (mysql_query (conn, "SELECT 1 FROM Beam WHERE name='F:NM3ION' AND value>0 LIMIT 1"))
            {
                fprintf (stderr, "%s\n", qryString);
                fprintf (stderr, "F:NM3ION Check Error: %s\n", mysql_error (conn) );
                return 1;
            }
            res = mysql_store_result(conn);
            rowcount = mysql_num_rows(res);
            mysql_free_result(res);

            if (rowcount == 1)
            {
                found = 1;
                sprintf(intensity_field,"F:NM3ION");
            }
        } else {
            found = 1;
            sprintf(intensity_field,"F:NM3SEM");
        }
    } else {
        found = 1;
        sprintf(intensity_field,"S:G2SEM");
    }
        
    if (found)
    {    
        sprintf (qryString, 
                 "UPDATE Spill s "
                 "INNER JOIN "
                 "( "
                 " SELECT b.spillID, "
                 "   ROUND(m.value*(b.QIESum - b.trigger_sum_no_inhibit - "
                 "                  b.inhibit_block_sum)/b.QIESum) "
                 "   AS `liveProton` "
                 " FROM BeamDAQ b "
                 " INNER JOIN Beam m USING(spillID) "
                 " WHERE name = '%s' "
                 " AND b.QIESum > 0 "
                 " AND b.QIESum > (b.trigger_sum_no_inhibit + b.inhibit_block_sum) "
                 ") t1 "
                 "USING (spillID) "
                 "SET s.liveProton = t1.liveProton", intensity_field);

        if ( mysql_query (conn,qryString) )
        {
            fprintf (stderr, "Live Proton Calculation Error: %s\n", mysql_error (conn) );
            return 1;
        }
    }

    // END LIVE PROTON CALCULATION -----------------------------------------------
    
    // SPILL, HIT, TRIGGERHIT COUNT SUMMARY ----------------------------------------------- 

    if ( mysql_query (conn, "SELECT spillCount, hitCount, triggerHitCount " 
                      "FROM ( SELECT COUNT(*) AS `spillCount` FROM Spill ) s, "
                      "( SELECT COUNT(*) as `hitCount` FROM Hit ) h, "
                      "( SELECT COUNT(*) AS `triggerHitCount` FROM TriggerHit ) t") )
    {
        fprintf (stderr, "Spill, Hit, TriggerHit Count Query Error: %s\n", mysql_error (conn) );
        return 1;
    }

    res = mysql_store_result (conn);

    if (mysql_num_rows (res) > 0)
    {
        row = mysql_fetch_row (res);
        sprintf (qryString, "UPDATE summary.Run2Summary SET spillCount=%i, hitCount=%i, triggerHitCount=%i "
                 "WHERE runID=%i",
                 atoi (row[0]), atoi (row[1]), atoi (row[2]), runNum);

        if ( mysql_query (conn, qryString) )
        {
            fprintf (stderr, "Update Run2Summary SpillCount HitCount TriggerHitCount Error (Ignoring Error): %s\n",
                    mysql_error (conn) );
        }

        sprintf (qryString, "INSERT INTO Run VALUES "
                 "(%i, \"spillCount\", %s),"
                 "(%i, \"hitCount\", %s),"
                 "(%i, \"triggerHitCount\", %s)",
                 runNum, row[0], runNum, row[1], runNum, row[2]);

        if ( mysql_query (conn, qryString) )
        {
            fprintf (stderr, "Update SpillCount HitCount TriggerHitCount Error: %s\n", mysql_error (conn) );
            return 1;
        }
    }

    mysql_free_result (res);
    
    // END SPILL, HIT, TRIGGERHIT COUNT SUMMARY ----------------------------------------------- 
    
    // LOG TABLE UPDATES ----------------------------------------------- 

    if(mysql_ping(conn))
    {
        printf("MySQL server is gone, and even Ping couldn't bring it back...\n");
    }

    if (args.nohit)
    {
        sprintf (qryString, "UPDATE log.production "
                 "SET decoded = 2, "
                 "productionEnd = NOW() "
                 "WHERE id = %i",
                 log_id);
    }
    else 
    {
        sprintf (qryString, "UPDATE log.production "
                 "SET decoded = 1, "
                 "productionEnd = NOW() "
                 "WHERE id = %i",
                 log_id);
    }

    if (mysql_query (conn, qryString) )
    {
        fprintf (stderr, "Error setting log.production entry to decoded=1: %s\n", mysql_error (conn) );
        return 1;
    }

    if (args.prod)
    {
        if(mysql_ping(conn))
        {
            printf("MySQL server is gone, and even Ping couldn't bring it back...\n");
        }
        if (args.nohit)
        {
            sprintf (qryString, "UPDATE summary.production "
                     "SET decoded = 2, "
                     "productionEnd = NOW() "
                     "WHERE id = %i",
                     prod_log_id);
        }
        else
        {
            sprintf (qryString, "UPDATE summary.production "
                     "SET decoded = 1, "
                     "productionEnd = NOW() "
                     "WHERE id = %i",
                     prod_log_id);
        }

        if (mysql_query (conn, qryString) )
        {
            fprintf (stderr, "Error setting summary.production log: %s\n", mysql_error (conn) );
            return 1;
        }
    }
    
    // END LOG TABLE UPDATES ----------------------------------------------- 
    
    // ENABLE TABLE INDEXING ----------------------------------------------- 

    if (verbose) fprintf(stdout, "Enabling table indexes...\n");
    if (!args.online)
    {

        sprintf(qryString,"mysql --reconnect -h %s -u production -pxxxxxxxxxxxx -P %i %s -e "
                "\"ALTER TABLE TriggerHit ENABLE KEYS\" &", args.server, PORT, args.schema);
        system(qryString);

        sprintf(qryString,"mysql --reconnect -h %s -u production -pxxxxxxxxxxxx -P %i %s -e "
                "\"ALTER TABLE Hit ENABLE KEYS\" &", args.server, PORT, args.schema);
        system(qryString);

    }

    sprintf(qryString,"mysql --reconnect -h %s -u production -pxxxxxxxxxxxx -P %i %s -e "
            "\"OPTIMIZE LOCAL TABLE Run, Spill, Event, Scaler, TriggerRoadsTHitEvent\" > /dev/null &",
            args.server, PORT, args.schema);
    system(qryString);

    // END ENABLE TABLE INDEXING ----------------------------------------------- 

    timeEnd = time (NULL);
    timeTotal += (double) ( timeEnd - timeStart );

    mysql_close (conn);
    close_coda_file();

    if (verbose)
    {
        printf ("\nTotal v1495 events: %i, Good: %i, 0xD1AD: %i, 0xD2AD: %i, "
                "0xD3AD: %i\n\n",
                hitStorage->all1495, hitStorage->good1495, hitStorage->d1ad1495,
                hitStorage->d2ad1495, hitStorage->d3ad1495);
        printf ("%i Physics Events Decoded\n", eventStorage->eventTotal);
        printf ("Average Rate: %f Events/s\n",
                ( (double) eventStorage->eventTotal) / (timeTotal) );
        printf ("Average Rate: %i Hits at %f Hit rows/s\n",
                hitStorage->hit_count, ( (double) hitStorage->hit_count) / (timeTotal) );
        printf ("Average Rate: %i TriggerHits at %f TriggerHit rows/s\n\n",
                hitStorage->thit_count, ( (double) hitStorage->thit_count) / (timeTotal) );
    }
    printf ("Real Time: %f\n", timeTotal);

    return 0;

}


int createSQL (MYSQL *conn, char *db, int online)
{
    // ================================================================
    // This function creates the schema and tables required to dump the decoded data
    //
    // Returns:	0 on success
    //		1 on error
    char qryString[10000];
    sprintf (qryString, "CREATE DATABASE IF NOT EXISTS %s", db);

    if ( mysql_query (conn, qryString) )
    {
        fprintf (stderr, "Database creation error: %s\n", mysql_error (conn) );
        return 1;
    }

    sprintf (qryString, "USE %s", db);

    if ( mysql_query (conn, qryString) )
    {
        fprintf (stderr, "Database selection error: %s\n", mysql_error (conn) );
        return 1;
    }

    if ( mysql_query (conn, "CREATE TABLE IF NOT EXISTS Run ("
                      "`runID` SMALLINT(5) UNSIGNED NOT NULL, "
                      "`name` CHAR(20), "
                      "`value` TEXT, "
                      "INDEX USING BTREE (name))") )
    {
        fprintf (stderr, "Run Table creation error: %s\n", mysql_error (conn) );
        return 1;
    }
    
    if (mysql_query (conn, "CREATE TABLE IF NOT EXISTS prestartInfo ( "
                     "`name` CHAR(20), "
                     "`value` TEXT )" ) )
    {
        fprintf (stderr, "Temp prestartInfo table creation Error: %s\n", mysql_error (conn) );
        return 1;
    }

    if ( mysql_query (conn, "CREATE TABLE IF NOT EXISTS Spill ("
                      "`spillID` INT(8) UNSIGNED NOT NULL, "
                      "`runID` SMALLINT(5) UNSIGNED NOT NULL, "
                      "`liveProton` BIGINT UNSIGNED, "
                      "`targetPos` TINYINT(3) UNSIGNED NOT NULL, "
                      "`dataQuality` BIGINT UNSIGNED DEFAULT 0, "
                      "`BOScodaEventID` INT(10) UNSIGNED NOT NULL, "
                      "`BOSvmeTime` INT(8) UNSIGNED NOT NULL, "
                      "`EOScodaEventID` INT(10) UNSIGNED, "
                      "`EOSvmeTime` INT(8) UNSIGNED, "
                      "`time` DATETIME, "
                      "INDEX USING BTREE (spillID), "
                      "INDEX USING BTREE (targetPos))") )
    {
        fprintf (stderr, "Spill table creation error: %s\n", mysql_error (conn) );
        return 1;
    }

    if ( mysql_query (conn, "CREATE TABLE IF NOT EXISTS Event ("
                      "`eventID` INT(10) UNSIGNED NOT NULL, "
                      "`codaEventID` INT(10) UNSIGNED NOT NULL, "
                      "`runID` SMALLINT(5) UNSIGNED NOT NULL, "
                      "`spillID` INT(8) UNSIGNED NOT NULL, "
                      "`NIM1` TINYINT UNSIGNED DEFAULT 0, "
                      "`NIM2` TINYINT UNSIGNED DEFAULT 0, "
                      "`NIM3` TINYINT UNSIGNED DEFAULT 0, "
                      "`NIM4` TINYINT UNSIGNED DEFAULT 0, "
                      "`NIM5` TINYINT UNSIGNED DEFAULT 0, "
                      "`MATRIX1` TINYINT UNSIGNED DEFAULT 0, "
                      "`MATRIX2` TINYINT UNSIGNED DEFAULT 0, "
                      "`MATRIX3` TINYINT UNSIGNED DEFAULT 0, "
                      "`MATRIX4` TINYINT UNSIGNED DEFAULT 0, "
                      "`MATRIX5` TINYINT UNSIGNED DEFAULT 0, "
                      "`RawMATRIX1` INT DEFAULT 0, "
                      "`AfterInhMATRIX1` INT DEFAULT 0, "
                      "`RawMATRIX2` INT DEFAULT 0, "
                      "`AfterInhMATRIX2` INT DEFAULT 0, "
                      "`RawMATRIX3` INT DEFAULT 0, "
                      "`AfterInhMATRIX3` INT DEFAULT 0, "
                      "`RawMATRIX4` INT DEFAULT 0, "
                      "`AfterInhMATRIX4` INT DEFAULT 0, "
                      "`RawMATRIX5` INT DEFAULT 0, "
                      "`AfterInhMATRIX5` INT DEFAULT 0, "
                      "`dataQuality` MEDIUMINT(8) UNSIGNED DEFAULT 0, "
                      "`vmeTime` INT(8) UNSIGNED NOT NULL, "
                      "INDEX USING BTREE (spillID), "
                      "INDEX USING BTREE (eventID) )") )
    {
        fprintf (stderr, "Event table creation error: %s\n", mysql_error (conn) );
        return 1;
    }

    sprintf(qryString, "CREATE TABLE IF NOT EXISTS Hit ("
                       "`hitID` BIGINT(20) UNSIGNED NOT NULL, "
                       "`spillID` INT(8) UNSIGNED NOT NULL, "
                       "`eventID` INT(10) UNSIGNED NOT NULL, "
                       "`detectorName` CHAR(16), "
                       "`elementID` SMALLINT, "
                       "`tdcTime` DECIMAL(7,3), "
                       "`inTime` TINYINT(1), "
                       "`masked` TINYINT(1), "
                       "`driftTime` DECIMAL(7,3), "
                       "`driftDistance` DECIMAL(5,4), "
                       "`resolution` DECIMAL(5,4), "
                       "`dataQuality` MEDIUMINT(8) UNSIGNED DEFAULT 0, ");

    if (online) 
    {
        sprintf(qryString, "%sINDEX USING BTREE (spillID), "
                           "INDEX USING BTREE (eventID), "
                           "INDEX USING BTREE (detectorName, elementID) ) ", qryString);
    }
    else
    {
        sprintf(qryString, "%sINDEX USING BTREE (eventID) ) ", qryString);
    }

    // This stores data from the Latches and TDC's
    if ( mysql_query (conn, qryString) )
    {
        fprintf (stderr, "Hit Table Creation Error: %s\n", mysql_error (conn) );
        return 1;
    }

    // This stores data from the Latches and TDC's
    if ( mysql_query (conn, "DROP TABLE IF EXISTS TriggerHit") )
    {
        fprintf (stderr, "TriggerHit Dropping Error: %s\n", mysql_error (conn) );
        return 1;
    }
    // This stores data that is processed by our v1495 trigger modules.
    //	They're kept separate in order to keep duplicate hits separated
    sprintf (qryString, "CREATE TABLE IF NOT EXISTS TriggerHit ("
                        "`hitID` BIGINT(20) UNSIGNED NOT NULL, "
                        "`spillID` INT(8) UNSIGNED NOT NULL, "
                        "`eventID` INT(10) UNSIGNED NOT NULL, "
                        "`detectorName` CHAR(16), "
                        "`elementID` SMALLINT(6), "
                        "`triggerLevel` TINYINT(3) UNSIGNED DEFAULT NULL, "
                        "`tdcTime` DECIMAL(7,3), "
                        "`inTime` TINYINT(1), "
                        "`errorBits` TINYINT(1) UNSIGNED DEFAULT 0, "
                        "`dataQuality` MEDIUMINT(8) UNSIGNED DEFAULT 0, ");

    if (online) 
    {
        sprintf(qryString, "%sINDEX USING BTREE (spillID), "
                           "INDEX USING BTREE (eventID), "
                           "INDEX USING BTREE (detectorName, elementID) ) ", qryString);
    }
    else
    {
        sprintf(qryString, "%sINDEX USING BTREE (eventID) ) ", qryString);
    }

    // This stores data from the Latches and TDC's
    if ( mysql_query (conn, qryString) )
    {
        fprintf (stderr, "TriggerHit Table Creation Error: %s\n", mysql_error (conn) );
        return 1;
    }

    if ( mysql_query (conn, "CREATE TABLE IF NOT EXISTS TriggerRoadsTHitEvent ( "
                      "`eventID` INT UNSIGNED NOT NULL, "
                      "`roadID` MEDIUMINT NOT NULL, "
                      "INDEX USING BTREE (eventID), "
                      "INDEX USING BTREE (roadID))") )
    {
        fprintf (stderr, "Create TriggerRoadsTHitEvent table error: %s\n", mysql_error (conn) );
        return -1;
    }

    // Contains data from our Scalers
    //"`runID` SMALLINT(5) UNSIGNED NOT NULL, "
    if ( mysql_query (conn, "CREATE TABLE IF NOT EXISTS Scaler ("
                      "`scalerID` BIGINT(20) UNSIGNED NOT NULL, "
                      "`spillID` INT(8) UNSIGNED NOT NULL, "
                      "`spillType` ENUM('BOS','EOS','OTHER'), "
                      "`scalerName` CHAR(16), "
                      "`value` INT(10) NOT NULL, "
                      "`dataQuality` MEDIUMINT(8) UNSIGNED DEFAULT 0, "
                      "INDEX USING BTREE (spillID), "
                      "INDEX USING BTREE (scalerName))") )
    {
        fprintf (stderr, "Scaler Table Creation Error: %s\n", mysql_error (conn) );
        return 1;
    }
    
    // The slow control data is directly uploaded to this table
    if ( mysql_query (conn, "CREATE TABLE IF NOT EXISTS SlowControl("
                            "`time` DATETIME NOT NULL, "
                            "`name` VARCHAR(64) NOT NULL, "
                            "`value` TEXT NOT NULL, "
                            "`type` VARCHAR(64), "
                            "INDEX USING BTREE (type(3)))") )
    {
        fprintf (stderr, "SlowControl table creation error: %s\n", mysql_error (conn) );
        return 1;
    }

    // Contains data from our BeamDAQ
    if ( mysql_query (conn, "CREATE TABLE IF NOT EXISTS BeamDAQ ("
                      "`spillID` INT UNSIGNED NOT NULL, "
                      "`timestamp` DATETIME NOT NULL, "
                      "`NM3ION` FLOAT NOT NULL, "
                      "`QIEsum` FLOAT NOT NULL, "
                      "`dutyfactor53MHz` DECIMAL(8,4) NOT NULL, "
                      "`inhibit_count` INT NOT NULL, "
                      "`inhibit_block_sum` FLOAT NOT NULL, "
                      "`trigger_count` INT NOT NULL, "
                      "`trigger_sum_no_inhibit` FLOAT NOT NULL, "
                      "`Inh_output_delay` INT NOT NULL, "
                      "`QIE_inh_delay` INT NOT NULL, "
                      "`Min_Inh_Width` INT NOT NULL, "
                      "`Inh_thres` INT NOT NULL, "
                      "`QIE_busy_delay` INT NOT NULL, "
                      "`Marker_delay`INT NOT NULL, "
                      "`QIE_phase_adjust` INT NOT NULL)") )
    {
        fprintf (stderr, "BeamDAQ Table Creation Error: %s\n", mysql_error (conn) );
        return 1;
    }

    // The slow control data is directly uploaded to this table
    if ( mysql_query (conn, "CREATE TABLE IF NOT EXISTS Beam("
                      "`runID` SMALLINT(5) UNSIGNED NOT NULL, "
                      "`spillID` INT(8) UNSIGNED NOT NULL, "
                      "`name` VARCHAR(64) NOT NULL, "
                      "`value` TEXT NOT NULL, "
                      "INDEX USING BTREE (runID), "
                      "INDEX USING BTREE (spillID), "
                      "INDEX USING BTREE (name))") )
    {
        fprintf (stderr, "Beam table creation error: %s\n", mysql_error (conn) );
        return 1;
    }

    if ( mysql_query (conn, "CREATE TABLE IF NOT EXISTS Environment ("
                      "`runID` SMALLINT(5) UNSIGNED NOT NULL, "
                      "`spillID` INT(8) UNSIGNED NOT NULL, "
                      "`name` VARCHAR(64) NOT NULL, "
                      "`value` TEXT NOT NULL, "
                      "INDEX USING BTREE (runID), "
                      "INDEX USING BTREE (spillID), "
                      "INDEX USING BTREE (name))") )
    {
        fprintf (stderr, "Beam table creation error: %s\n", mysql_error (conn) );
        return 1;
    }

    // The slow control data is directly uploaded to this table
    if ( mysql_query (conn, "CREATE TABLE IF NOT EXISTS Target("
                      "`runID` SMALLINT(5) UNSIGNED NOT NULL, "
                      "`spillID` INT(8) UNSIGNED NOT NULL, "
                      "`name` VARCHAR(64) NOT NULL, "
                      "`value` TEXT NOT NULL, "
                      "INDEX USING BTREE (runID), "
                      "INDEX USING BTREE (spillID), "
                      "INDEX USING BTREE (name))") )
    {
        fprintf (stderr, "Target table creation error: %s\n", mysql_error (conn) );
        return 1;
    }

    // The slow control data is directly uploaded to this table
    if ( mysql_query (conn, "CREATE TABLE IF NOT EXISTS HV("
                      "`runID` SMALLINT(5) UNSIGNED NOT NULL, "
                      "`spillID` INT(8) UNSIGNED NOT NULL, "
                      "`name` VARCHAR(64) NOT NULL, "
                      "`value` TEXT NOT NULL, "
                      "INDEX USING BTREE (runID), "
                      "INDEX USING BTREE (spillID), "
                      "INDEX USING BTREE (name))") )
    {
        fprintf (stderr, "HV table creation error: %s\n", mysql_error (conn) );
        return 1;
    }

    // This is the table into which the tdc board data is uploaded
    if ( mysql_query (conn, "CREATE TABLE IF NOT EXISTS tdcInfo ("
                      "`runID` SMALLINT(5) UNSIGNED NOT NULL, "
                      "`rocID` TINYINT(3) UNSIGNED NOT NULL, "
                      "`boardID` SMALLINT(5) UNSIGNED NOT NULL, "
                      "`TDCHardID` SMALLINT(5) UNSIGNED NOT NULL, "
                      "`fallingEdge` TINYINT(3) UNSIGNED NOT NULL, "
                      "`segmentation` SMALLINT(6) NOT NULL, "
                      "`multiHitElim` TINYINT(3) UNSIGNED NOT NULL, "
                      "`updating` TINYINT(3) UNSIGNED NOT NULL, "
                      "`elimWindow` FLOAT NOT NULL, "
                      "`selectWindow` FLOAT NOT NULL, "
                      "`lowSelectLimit` FLOAT NOT NULL, "
                      "`highSelectLimit` FLOAT NOT NULL, "
                      "INDEX USING BTREE (rocID, boardID) )"
                      "ENGINE=MyISAM") )
    {
        fprintf (stderr, "Create tempHit error: %s\n", mysql_error (conn) );
        return 1;
    }

    if ( mysql_query (conn, "CREATE TABLE IF NOT EXISTS QIE ( "
                      "`runID` SMALLINT(5) UNSIGNED NOT NULL, "
                      "`spillID` INT(8) UNSIGNED NOT NULL, "
                      "`eventID` INT(10) UNSIGNED NOT NULL, "
                      "`triggerCount` INT NOT NULL, "
                      "`turnOnset` INT NOT NULL, "
                      "`rfOnset` MEDIUMINT NOT NULL, "
                      "`RF-16` MEDIUMINT NOT NULL, "
                      "`RF-15` MEDIUMINT NOT NULL, "
                      "`RF-14` MEDIUMINT NOT NULL, "
                      "`RF-13` MEDIUMINT NOT NULL, "
                      "`RF-12` MEDIUMINT NOT NULL, "
                      "`RF-11` MEDIUMINT NOT NULL, "
                      "`RF-10` MEDIUMINT NOT NULL, "
                      "`RF-09` MEDIUMINT NOT NULL, "
                      "`RF-08` MEDIUMINT NOT NULL, "
                      "`RF-07` MEDIUMINT NOT NULL, "
                      "`RF-06` MEDIUMINT NOT NULL, "
                      "`RF-05` MEDIUMINT NOT NULL, "
                      "`RF-04` MEDIUMINT NOT NULL, "
                      "`RF-03` MEDIUMINT NOT NULL, "
                      "`RF-02` MEDIUMINT NOT NULL, "
                      "`RF-01` MEDIUMINT NOT NULL, "
                      "`RF+00` MEDIUMINT NOT NULL, "
                      "`RF+01` MEDIUMINT NOT NULL, "
                      "`RF+02` MEDIUMINT NOT NULL, "
                      "`RF+03` MEDIUMINT NOT NULL, "
                      "`RF+04` MEDIUMINT NOT NULL, "
                      "`RF+05` MEDIUMINT NOT NULL, "
                      "`RF+06` MEDIUMINT NOT NULL, "
                      "`RF+07` MEDIUMINT NOT NULL, "
                      "`RF+08` MEDIUMINT NOT NULL, "
                      "`RF+09` MEDIUMINT NOT NULL, "
                      "`RF+10` MEDIUMINT NOT NULL, "
                      "`RF+11` MEDIUMINT NOT NULL, "
                      "`RF+12` MEDIUMINT NOT NULL, "
                      "`RF+13` MEDIUMINT NOT NULL, "
                      "`RF+14` MEDIUMINT NOT NULL, "
                      "`RF+15` MEDIUMINT NOT NULL, "
                      "`RF+16` MEDIUMINT NOT NULL, "
                      "INDEX USING BTREE (eventID) )") )
    {
        fprintf (stderr, "Create QIE Table Error: %s\n", mysql_error (conn) );
        return 1;
    }

    if ( mysql_query (conn, "CREATE TABLE IF NOT EXISTS Occupancy ( "
                      "`runID` SMALLINT(5) UNSIGNED NOT NULL, "
                      "`spillID` INT(8) UNSIGNED NOT NULL, "
                      "`eventID` INT(10) UNSIGNED NOT NULL, "
                      "`D1` MEDIUMINT NOT NULL, "
                      "`D2` MEDIUMINT NOT NULL, "
                      "`D3` MEDIUMINT NOT NULL, "
                      "`H1` MEDIUMINT NOT NULL, "
                      "`H2` MEDIUMINT NOT NULL, "
                      "`H3` MEDIUMINT NOT NULL, "
                      "`H4` MEDIUMINT NOT NULL, "
                      "`P1` MEDIUMINT NOT NULL, "
                      "`P2` MEDIUMINT NOT NULL, "
                      "INDEX USING BTREE (eventID), "
                      "INDEX USING BTREE (spillID) )") )
    {
        fprintf (stderr, "Create QIE Table Error: %s\n", mysql_error (conn) );
        return 1;
    }

    return 0;
}

int runExists (MYSQL *conn, int runNumber)
{
    MYSQL_RES *res;
    MYSQL_ROW row;
    int exists = 0;
    char qryString[10000];
    sprintf (qryString, "SELECT COUNT(*) FROM Run WHERE runID=%i", runNumber);

    if ( mysql_query (conn, qryString) )
    {
        fprintf (stderr, "Table Check Error: %s\n", mysql_error (conn) );
        return 1;
    }

    // Store the query result
    res = mysql_store_result (conn);
    // Grab the first (and only) row
    row = mysql_fetch_row (res);
    // Store the value
    exists = atoi (row[0]);

    if ( exists > 1 )
    {
        exists = 1;
    }

    mysql_free_result (res);
    return exists;
}

int feeEventSQL (MYSQL *conn, int physicsEvent[70000])
{
    unsigned int roc, board, multihit_elim_enabled, updating_enabled;
    unsigned int falling_enabled, segmentation;
    //unsigned int event_buffer;
    unsigned int selectWindow, lowLimit, highLimit;
    int window;
    int tdcHardID;
    int size, i;
    char qryString[10000];
    //for(i=0;i<40;i++){
    //    printf("%.8x\n",physicsEvent[i]);
    //}
    size = physicsEvent[0];
    roc = physicsEvent[2];
    i = 3;

    while (i < size)
    {
        // physicsEvent[i] = 0xe906f011 // ignore this flag
        i++;
        // This word contains only the boardID
        board = get_hex_bits (physicsEvent[i], 7, 4);
        tdcHardID = get_hex_bits (physicsEvent[i], 1, 2);
        i++;
        // This is the TDC registry word
        falling_enabled = get_hex_bit (physicsEvent[i], 2);
        segmentation = get_hex_bit (physicsEvent[i], 1);

        if ( segmentation == 0 )
        {
            segmentation = 512;
        }

        if ( segmentation == 2 )
        {
            segmentation = 1024;
        }

        if ( segmentation == 6 )
        {
            segmentation = 2048;
        }

        //event_buffer = get_hex_bit (physicsEvent[i], 0) + 1;
        i++;
        // Another TDC registry word
        multihit_elim_enabled = get_bin_bit (physicsEvent[i], 23);
        updating_enabled = get_bin_bit (physicsEvent[i], 22);

        if (multihit_elim_enabled)
        {
            window = get_bin_bits (physicsEvent[i], 21, 6);
            window = 4 * (4 + window);
        }
        else
        {
            window = 0;
        }

        i++;
        lowLimit = get_hex_bits (physicsEvent[i], 2, 3);
        highLimit = get_hex_bits (physicsEvent[i], 6, 3);

        if (get_hex_bit (physicsEvent[i], 7) == 8)
        {
            selectWindow = 1;
        }
        else
        {
            selectWindow = 0;
        }

        i++;
        sprintf (qryString, "INSERT INTO tdcInfo VALUES (%i, %i, %i, %i, %i, %i, %i, %i, %i, %i, %i, %i)",
                 runNum, roc, board, tdcHardID, falling_enabled, segmentation,
                 multihit_elim_enabled, updating_enabled, window, selectWindow, lowLimit, highLimit);

        if (mysql_query (conn, qryString) )
        {
            fprintf (stderr, "Error updating tdcInfo with FEE data: %s", mysql_error (conn) );
            return 1;
        }
    }

    return 0;
}

int feePrescaleSQL (MYSQL *conn, int physicsEvent[70000])
{
    char qryString[1000], qryString2[1000];
    int x, trigger[10], feeTriggerBits;
    
    feeTriggerBits = physicsEvent[4];

    for (x = 0; x < 10; x++)
    {
        trigger[x] = get_bin_bit (feeTriggerBits, x);
    }

    if (runNum >= 4923)
    {
        sprintf (qryString, "INSERT INTO Run VALUES (%i, \"NIM1Enabled\", %i),"
                 "(%i, \"NIM2Enabled\", %i),"
                 "(%i, \"NIM3Enabled\", %i),"
                 "(%i, \"NIM4Enabled\", %i),"
                 "(%i, \"NIM5Enabled\", %i),"
                 "(%i, \"MATRIX1Enabled\", %i),"
                 "(%i, \"MATRIX2Enabled\", %i),"
                 "(%i, \"MATRIX3Enabled\", %i),"
                 "(%i, \"MATRIX4Enabled\", %i),"
                 "(%i, \"MATRIX5Enabled\", %i)",
                 runNum, trigger[5],
                 runNum, trigger[6],
                 runNum, trigger[7],
                 runNum, trigger[8],
                 runNum, trigger[9],
                 runNum, trigger[0],
                 runNum, trigger[1],
                 runNum, trigger[2],
                 runNum, trigger[3],
                 runNum, trigger[4]);
        sprintf (qryString2, "UPDATE summary.Run2Summary SET NIM1Enabled=%i, NIM2Enabled=%i, "
                 "NIM3Enabled=%i, NIM4Enabled=%i, "
                 "NIM5Enabled=%i, MATRIX1Enabled=%i, MATRIX2Enabled=%i, MATRIX3Enabled=%i, MATRIX4Enabled=%i, "
                 "MATRIX5Enabled=%i WHERE runID=%i",
                 trigger[5], trigger[6],
                 trigger[7], trigger[8], trigger[9],
                 trigger[0], trigger[1], trigger[2],
                 trigger[3], trigger[4], runNum);
    }
    else
    {
        sprintf (qryString, "INSERT INTO Run VALUES (%i, \"NIM1Enabled\", %i),"
                 "(%i, \"NIM2Enabled\", %i),"
                 "(%i, \"NIM3Enabled\", %i),"
                 "(%i, \"NIM4Enabled\", %i),"
                 "(%i, \"NIM5Enabled\", %i),"
                 "(%i, \"MATRIX1Enabled\", %i),"
                 "(%i, \"MATRIX2Enabled\", %i),"
                 "(%i, \"MATRIX3Enabled\", %i),"
                 "(%i, \"MATRIX4Enabled\", %i),"
                 "(%i, \"MATRIX5Enabled\", %i)",
                 runNum, trigger[0],
                 runNum, trigger[1],
                 runNum, trigger[2],
                 runNum, trigger[3],
                 runNum, trigger[4],
                 runNum, trigger[5],
                 runNum, trigger[6],
                 runNum, trigger[7],
                 runNum, trigger[8],
                 runNum, trigger[9]);
        sprintf (qryString2, "UPDATE summary.Run2Summary SET NIM1Enabled=%i, NIM2Enabled=%i, "
                 "NIM3Enabled=%i, NIM4Enabled=%i, NIM5Enabled=%i, MATRIX1Enabled=%i, "
                 "MATRIX2Enabled=%i, MATRIX3Enabled=%i, MATRIX4Enabled=%i, "
                 "MATRIX5Enabled=%i WHERE runID=%i",
                 trigger[0], trigger[1],
                 trigger[2], trigger[3], trigger[4],
                 trigger[5], trigger[6], trigger[7],
                 trigger[8], trigger[9], runNum);
    }

    if ( mysql_query (conn, qryString) )
    {
        fprintf (stderr, "Run TriggerBits Update Error: %s\n", mysql_error (conn) );
        return 1;
    }

    if ( mysql_query (conn, qryString2) )
    {
        fprintf (stderr, "Run2Summary TriggerBits Update Error (Ignoring Error): %s\n", mysql_error (conn) );
    }

    if (runNum >= 4923)
    {
        sprintf (qryString, "INSERT INTO Run VALUES "
                 "(%i, \"MATRIX1Prescale\", %i),"
                 "(%i, \"MATRIX2Prescale\", %i),"
                 "(%i, \"MATRIX3Prescale\", %i),"
                 "(%i, \"MATRIX4Prescale\", %i),"
                 "(%i, \"MATRIX5Prescale\", %i),"
                 "(%i, \"NIM1Prescale\", %i),"
                 "(%i, \"NIM2Prescale\", %i),"
                 "(%i, \"NIM3Prescale\", %i)",
                 runNum, physicsEvent[5],
                 runNum, physicsEvent[6],
                 runNum, physicsEvent[7],
                 runNum, physicsEvent[8],
                 runNum, physicsEvent[9],
                 runNum, physicsEvent[10],
                 runNum, physicsEvent[11],
                 runNum, physicsEvent[12]);
        sprintf (qryString2, "UPDATE summary.Run2Summary SET MATRIX1Prescale=%i, MATRIX2Prescale=%i, "
                 "MATRIX3Prescale=%i, MATRIX4Prescale=%i, MATRIX5Prescale=%i, NIM1Prescale=%i, "
                 "NIM2Prescale=%i, NIM3Prescale=%i, NIM4Prescale=NULL, NIM5Prescale=NULL "
                 "WHERE runID=%i",
                 physicsEvent[5], physicsEvent[6], physicsEvent[7], physicsEvent[8], physicsEvent[9],
                 physicsEvent[10], physicsEvent[11], physicsEvent[12], runNum);
    }
    else
    {
        sprintf (qryString, "INSERT INTO Run VALUES "
                 "(%i, \"MATRIX1Prescale\", %i),"
                 "(%i, \"MATRIX2Prescale\", %i),"
                 "(%i, \"MATRIX3Prescale\", %i),"
                 "(%i, \"NIM1Prescale\", %i),"
                 "(%i, \"NIM2Prescale\", %i),"
                 "(%i, \"NIM3Prescale\", %i),"
                 "(%i, \"NIM4Prescale\", %i),"
                 "(%i, \"NIM5Prescale\", %i)",
                 runNum, physicsEvent[10],
                 runNum, physicsEvent[11],
                 runNum, physicsEvent[12],
                 runNum, physicsEvent[5],
                 runNum, physicsEvent[6],
                 runNum, physicsEvent[7],
                 runNum, physicsEvent[8],
                 runNum, physicsEvent[9]);
        sprintf (qryString2, "UPDATE summary.Run2Summary SET MATRIX1Prescale=%i, MATRIX2Prescale=%i, "
                 "MATRIX3Prescale=%i, MATRIX4Prescale=NULL, MATRIX5Prescale=NULL, NIM1Prescale=%i, "
                 "NIM2Prescale=%i, NIM3Prescale=%i, NIM4Prescale=%i, NIM5Prescale=%i "
                 "WHERE runID=%i",
                 physicsEvent[10], physicsEvent[11], physicsEvent[12],
                 physicsEvent[5], physicsEvent[6], physicsEvent[7], physicsEvent[8], physicsEvent[9], runNum);
    }

    if (mysql_query (conn, qryString) )
    {
        fprintf (stderr, "Error updating Prescale Factors in Run table: %s", mysql_error (conn) );
        return 1;
    }

    if (mysql_query (conn, qryString2) )
    {
        fprintf (stderr, "Error updating Prescale Factors in Run2Summary table (Ignoring Error): %s", mysql_error (conn) );
    }

    return 0;
}

int feeQIESQL (int physicsEvent[70000], int j, Event* e)
{
    unsigned int rf_vals[33];
    int i;
    int qieEventLength;
    int submitQIE, endQieEvent;
    int numWords;
    unsigned int triggerCount, turnOnset, rfOnset;
    // int boardID;

    while ( physicsEvent[j] == (int)0xe906e906 )
    {
        j++;
    }

    // boardID = get_hex_bits (physicsEvent[j], 7, 4);
    qieEventLength = get_hex_bits (physicsEvent[j], 3, 4);
    endQieEvent = ( j ) + qieEventLength;
    j++;

    while ( physicsEvent[j] == (int)0xe906e906 )
    {
        j++;
        endQieEvent++;
    }

    //int x;
    //printf("\n\n");
    //for (x=j-4;x<endQieEvent;x++){
    //    printf("%.8x\n", physicsEvent[x]);
    //}
    //printf("\n\n");

    numWords = get_hex_bits (physicsEvent[j], 7, 4);
    // numWords = physicsEvent[j]; // For reversed QIE issue

    if ( numWords == 0 )
    {
        submitQIE = 0;
        e->qieFlag = 0;

        // ROOT FILE HANDLING
        rawEvent->setTurnID(-1);
        rawEvent->setRFID(-1);
        // ------------------
    }
    else
    {
        submitQIE = 1;
    }

    if ( numWords == 0x2C )
    {
        j = j + 5;
    }

    j++;

    while ( physicsEvent[j] == (int)0xe906e906 )
    {
        j++;
        endQieEvent++;
    }

    triggerCount = physicsEvent[j] | ( get_hex_bits (physicsEvent[j+1], 7, 4) );
    //triggerCount = ( physicsEvent[j] << 16 ) | physicsEvent[j + 1]; // For reversed QIE issue
    j = j + 2;

    while ( physicsEvent[j] == (int)0xe906e906 )
    {
        j++;
        endQieEvent++;
    }

    turnOnset = physicsEvent[j] | ( get_hex_bits (physicsEvent[j+1], 7, 4) );
    //turnOnset = ( physicsEvent[j] << 16 ) | physicsEvent[j + 1]; // For reversed QIE issue
    j = j + 2;

    while ( physicsEvent[j] == (int)0xe906e906 )
    {
        j++;
        endQieEvent++;
    }
    
    rfOnset = ( get_hex_bits(physicsEvent[j],7,4) );
    // rfOnset = ( physicsEvent[j] ); // For reversed QIE issue
    // ROOT FILE HANDLING
    if (submitQIE)
    {
        rawEvent->setTurnID(turnOnset);
        rawEvent->setRFID(rfOnset);
    }
    // ------------------
    j++;
    i = 0;

    while ( (i <= 32) && (physicsEvent[j] != (int)0xe906c0da) )
    {
        if ( physicsEvent[j] != (int)0xe906e906 )
        {
            rf_vals[i] = ( get_hex_bits(physicsEvent[j],7,4) );
            // rf_vals[i] = ( physicsEvent[j] ); // For reversed QIE issue

            // ROOT FILE HANDLING
            if (submitQIE)
            {
                rawEvent->setIntensity(i, rf_vals[i]);
            }
            else
            {
                rawEvent->setIntensity(i, -1);
            }
            // ------------------

            i++;
        }
        else
        {
            endQieEvent++;
        }

        j++;
    }

    if ( submitQIE )
    {
        e->qieFlag = 1;
        e->triggerCount = triggerCount;
        e->turnOnset = turnOnset;
        e->rfOnset = rfOnset;
        for (int j = 0; j < 33 ; j++)
        {
            e->rf[j] = rf_vals[j];
        }
    } //else
    //{
        //e->qieFlag = 0;
        //e->triggerCount = -1;
        //e->turnOnset = -1;
        //e->rfOnset = -1;
        //for (int j = 0; j < 33 ; j++)
        //{
        //    e->rf[j] = -1;
        //}
    //}

    while ( (physicsEvent[j] != (int)0xe906c0da) && (j < endQieEvent) )
    {
        j++;
    }

    j++;
    return j;
}


int prestartSQL (MYSQL *conn, int physicsEvent[70000], DocoptArgs args)
{
    // ================================================================
    //
    //
    // This function hadles the 'Go Event' CODA Events, storing its
    //	values appropriately.
    //
    // Returns: 0 on success
    //	    1 on MySQL error
    //
    MYSQL_RES *res;
    MYSQL_ROW row;
    int rowcount;
    // int evLength = 0;
    // int prestartCode = 0;
    char qryString[10000];
    const char *tblList[16];
    // int runType = 0;
    int runTime;
    int x;
    
    tblList[0] = "Run";
    tblList[1] = "Event";
    tblList[2] = "Hit";
    tblList[3] = "TriggerHit";
    tblList[4] = "Scaler";
    tblList[5] = "Spill";
    tblList[6] = "Beam";
    tblList[7] = "HV";
    tblList[8] = "Target";
    tblList[9] = "TriggerRoadsTHitEvent";
    tblList[10] = "tdcInfo";
    tblList[11] = "QIE";
    tblList[12] = "BeamDAQ";
    tblList[13] = "Environment";
    tblList[14] = "BeamDAQ";
    tblList[15] = "Occupancy";
    
    // evLength = physicsEvent[0];
    // prestartCode = physicsEvent[1];
    runTime = physicsEvent[2];
    runNum = physicsEvent[3];
    // runType = physicsEvent[4];
    timeEnd = time (NULL);
    timeTotal += (double) ( timeEnd - timeStart );

    // Check if this run already exists in the data
    if (runExists (conn, runNum) )
    {
        if (verbose) 
        {
            printf (">> Clearing data regarding RunID = %i\n>> This may "
                "take a few moments...\n", runNum);
        }

        for (x = 0; x < 16; x++)
        {
            sprintf (qryString, "TRUNCATE TABLE %s", tblList[x]);

            if (mysql_query (conn, qryString) )
            {
                fprintf (stderr, "%s\n", qryString);
                fprintf (stderr, ">> Error clearing out %s data: %s\n", tblList[x], mysql_error (conn) );
                return 1;
            }
        }

        if (verbose)
        { 
            printf (">> RunID = %i data has been cleared.\n>> Decoding...\n", runNum); 
        }
    }

    sprintf (qryString, "SELECT COUNT(*) FROM summary.Run2Summary WHERE runID=%i", runNum);
    if (mysql_query (conn, qryString) )
    {
        fprintf (stderr, ">> Error checking for previous Run2Summary Entry (Ignoring error): %s\n", mysql_error (conn) );
    }
    res = mysql_store_result (conn);

    if ((row = mysql_fetch_row (res)) )
    {
        if (atoi (row[0]) == 0)
        {
            mysql_free_result (res);
            sprintf (qryString, "INSERT INTO summary.Run2Summary (runID, runDate, runTime) SELECT "
                     "%i, DATE(FROM_UNIXTIME(%i)), TIME(FROM_UNIXTIME(%i))",
                     runNum, runTime, runTime);

            if (mysql_query (conn, qryString) )
            {
                fprintf (stderr, "%s\n", qryString);
                fprintf (stderr, ">> Error creating Run2Summary Entry (Ignoring error): %s\n", mysql_error (conn) );
            }
        }
        else
        {
            mysql_free_result (res);
        }
    }
    else
    {
        mysql_free_result (res);
    }

    sprintf (qryString, "INSERT INTO Run VALUES (%i, \"runDate\", DATE(FROM_UNIXTIME(%i))), "
             "(%i, \"runTime\", TIME(FROM_UNIXTIME(%i)))",
             runNum, runTime, runNum, runTime);

    if (mysql_query (conn, qryString) )
    {
        fprintf (stderr, "%s\n", qryString);
        fprintf (stderr, ">> Error setting run time: %s\n", mysql_error (conn) );
        return 1;
    }

    res = mysql_store_result (conn);
    mysql_free_result (res);

    sprintf (qryString, "INSERT INTO log.production (run, production, productionStart, decoded, "
             "sampling, jtracked, ktracked) "
             "VALUES (%i, \"%s\", NOW(), 0, %i, 0, 0)",
             runNum, args.schema, atoi(args.sampling));

    if (mysql_query (conn, qryString) )
    {
        fprintf (stderr, "Error setting production log: %s\n", mysql_error (conn) );
        return 1;
    }

    log_id = mysql_insert_id(conn);

    if (args.prod)
    {
        sprintf (qryString, "SELECT id FROM summary.production WHERE run=%i AND production='%s' ORDER BY id DESC",
                 runNum, args.schema);
        if (mysql_query (conn, qryString) )
        {
            fprintf (stderr, "Error production log for previous entries: %s\n", mysql_error (conn) );
            return 1;
        }

        res = mysql_store_result(conn);
        rowcount = mysql_num_rows(res);

        if (rowcount == 1)
        {
            row = mysql_fetch_row (res);
            prod_log_id = atoi(row[0]);
            mysql_free_result (res);

            sprintf (qryString, "UPDATE summary.production SET productionStart=NOW(), productionEnd=NULL, "
                                "decoded=0, sampling=%i WHERE id=%i",
                                atoi(args.sampling), prod_log_id);

            if (mysql_query (conn, qryString) )
            {
                fprintf (stderr, "Error setting production log: %s\n", mysql_error (conn) );
                return 1;
            }
        }
        else if (rowcount > 1)
        {
            printf("Multiple entries in summary.production! Please address this!\n");
            row = mysql_fetch_row (res);
            prod_log_id = atoi(row[0]);

            mysql_free_result (res);

            sprintf (qryString, "UPDATE summary.production SET productionStart=NOW(), productionEnd=NULL, "
                     "decoded=0, sampling=%i WHERE id=%i",
                     atoi(args.sampling), prod_log_id);

            if (mysql_query (conn, qryString) )
            {
                fprintf (stderr, "Error setting production log: %s\n", mysql_error (conn) );
                return 1;
            }
        }
        else if (rowcount == 0)
        {
            mysql_free_result (res);

            sprintf (qryString, "INSERT INTO summary.production (run, production, productionStart, "
                     "decoded, sampling, jtracked, ktracked) "
                     " VALUES (%i, \"%s\", NOW(), 0, %i, 0, 0)",
                     runNum, args.schema, atoi(args.sampling));

            if (mysql_query (conn, qryString) )
            {
                fprintf (stderr, "Error setting production log: %s\n", mysql_error (conn) );
                return 1;
            }

            prod_log_id = mysql_insert_id(conn);
        }
    }

    res = mysql_store_result (conn);
    mysql_free_result (res);

    timeStart = time (NULL);

    return 0;
}

int prestartInfo (MYSQL *conn, int physicsEvent[70000], char* dir)
{
    char qryString [10000] = "";
    int i, x;
    int pid;
    unsigned int number;
    FILE* fp;
    char outputFileName[1024];
    int evLength = 0;

    pid = getpid();
    evLength = physicsEvent[0];

    sprintf (outputFileName, "%s/.prestartinfo.%i.temp", dir, pid);
    if (file_exists (outputFileName) )
    {
        remove (outputFileName);
    }

    fp = fopen (outputFileName, "w");
    if (fp == NULL)
    {
        fprintf (stderr, "Can't open output file %s!\n",
                 outputFileName);
        exit (1);
    }

    for (x = 4; x < (evLength + 1); x++)
    {
        number = physicsEvent[x];
        for (i = 0; i < 4; i++)
        {
            fprintf (fp, "%c", (number & 0xFF) );
            number = number >> 8;
        }
    }

    fclose (fp);

    sprintf (qryString, "LOAD DATA LOCAL INFILE '%s' INTO TABLE prestartInfo "
             "FIELDS TERMINATED BY ' ' OPTIONALLY ENCLOSED BY '\\''", outputFileName);
    if (mysql_query (conn, qryString) )
    {
        fprintf (stderr, "Presart Info LOAD DATA Error: %s\n", mysql_error (conn) );
        return 1;
    }

    sprintf (qryString, "INSERT INTO Run SELECT %i, name, value FROM prestartInfo", runNum);
    if (mysql_query (conn, qryString) )
    {
        fprintf (stderr, "Insert Prestart Info into Run table error: %s\n", mysql_error (conn) );
        return 1;
    }

    if (mysql_query (conn, "DELETE FROM prestartInfo") )
    {
        fprintf (stderr, "Dropping prestartInfo temporary table error: %s\n", mysql_error (conn) );
        return 1;
    }

    return 0;

}

int goEventSQL (int physicsEvent[70000])
{
    // ================================================================
    //
    // This function hadles the 'Go Event' CODA Events, storing its
    //	values appropriately.
    //
    // Returns: 0

    // There are no actions to be taken here, or any important values to store.

    // int goEventTime = 0;
    // int numEventsInRunThusFar = 0;
    // int evLength = 0;
    // int goEventCode = 0;
    //// evLength is the important field to read, as it tells us
    //// 	when to stop reading the codaEvents
    // evLength = physicsEvent[0];
    // goEventCode = physicsEvent[1];
    // goEventTime = physicsEvent[2];
    //// physicsEvent[3] is reserved as 0
    //numEventsInRunThusFar = physicsEvent[4];
    return 0;
}


void showWarnings (MYSQL *conn)
{
    MYSQL_RES *res;
    MYSQL_ROW row;
    int num_cols, i;
    mysql_query (conn, "SHOW WARNINGS");
    res = mysql_store_result (conn);
    num_cols = mysql_num_fields (res);
    printf ("\n\nLevel\tCode\tMessage\n");

    while ((row = mysql_fetch_row (res) ))
    {
        for (i = 0; i < num_cols; i++)
        {
            printf ("%s\t", row[i]);
        }

        printf ("\n");
    }

    printf ("\n\n");
    mysql_free_result (res);
    return;
    //free(sqlString);
}

int eventRunDescriptorSQL (MYSQL *conn, int physicsEvent[70000])
{
    char descriptor [10000] = "";
    char qryString [10000] = "";
    int i, x;
    unsigned int number;
    int evLength = 0;

    evLength = physicsEvent[0];

    // Assemble the descriptor from the hex
    for (x = 4; x < (evLength + 1); x++)
    {
        number = physicsEvent[x];

        for (i = 0; i < 4; i++)
        {
            if ( (number & 0xFF) == 0x22)
            {
                sprintf (descriptor, "%s'", descriptor);
            }
            else
            {
                sprintf (descriptor, "%s%c", descriptor, (number & 0xFF) );
            }

            number = number >> 8;
        }
    }

    sprintf (qryString, "INSERT INTO Run VALUES (%i, \"runDescriptor\",\"%s\")", runNum, descriptor);

    if (mysql_query (conn, qryString) )
    {
        fprintf (stderr, "%s\n", qryString);
        fprintf (stderr, "Run Descriptor Update Error: %s\n", mysql_error (conn) );
        return 1;
    }

    // Create and execute the query that updates the descriptor field
    sprintf (qryString, "UPDATE summary.Run2Summary SET runDescriptor = \"%s\" WHERE runID = %i",
             descriptor, runNum);

    if (mysql_query (conn, qryString) )
    {
        fprintf (stderr, "%s\n", qryString);
        fprintf (stderr, "Run2Summary Descriptor Update Error (Ignoring Error): %s\n", mysql_error (conn) );
    }

    return 0;
}

int eventSlowSQL (MYSQL *conn, int physicsEvent[70000], char* dir)
{
    // ================================================================
    //
    // This function takes the Slow Control Physics Event, which is purely
    //	a blob of ASCII presented in the form of parsed and reversed
    //	unsigned int's.  It slaps together the whole event into one
    //	big string of hex values, parses it into 2-digit/1-char chunks,
    //	flips every group of four characters, and writes them to file.
    //	After being written to file, they are loaded up to the MySQL server
    //	via the LOAD DATA LOCAL INFILE function.
    //
    // Returns:	0 if successful
    //		1 on error
    MYSQL_RES *res1;
    MYSQL_RES *res2;
    MYSQL_RES *res4;
    MYSQL_ROW row;
    char outputFileName[5000];
    char qryString[60000];
    char beamSQL[1000];
    char envSQL[1000];
    char targ1SQL[1000];
    char targ2SQL[1000];
    char hvSQL[1000];
    int i, pid, x;
    FILE *fp;
    unsigned int number;
    int evLength;

    evLength = physicsEvent[0];
    targPos = 0;
    pid = getpid();
    sprintf (outputFileName, "%s/.codaSlowControl.%i.temp", dir, pid);

    // Open the temp file
    if (file_exists (outputFileName) )
    {
        remove (outputFileName);
    }

    fp = fopen (outputFileName, "w");

    if (fp == NULL)
    {
        fprintf (stderr, "Can't open output file %s!\n",
                 outputFileName);
        exit (1);
    }

    for (x = 4; x < (evLength + 1); x++)
    {
        number = physicsEvent[x];

        for (i = 0; i < 4; i++)
        {
            fprintf (fp, "%c", (number & 0xFF) );
            //printf("%c",(number&0xFF));
            number = number >> 8;
        }
    }

    fclose (fp);
    
    // The slow control data is directly uploaded to this table
    if ( mysql_query (conn, "CREATE TABLE IF NOT EXISTS SlowControl("
                            "`time` DATETIME NOT NULL, "
                            "`name` VARCHAR(64) NOT NULL, "
                            "`value` TEXT NOT NULL, "
                            "`type` VARCHAR(64), "
                            "INDEX USING BTREE (type(3)))") )
    {
        fprintf (stderr, "SlowControl table creation error: %s\n", mysql_error (conn) );
        return 1;
    }
    
    // Assemble the MySQL query to load the file into the server.
    // 	The LOCAL means the file is on the local machine
    sprintf (qryString, "LOAD DATA LOCAL INFILE '%s' INTO TABLE SlowControl "
             "FIELDS TERMINATED BY ' ' OPTIONALLY ENCLOSED BY '\\''", outputFileName);

    // Submit the query to the server
    if (mysql_query (conn, qryString) )
    {
        fprintf (stderr, "%s\n", qryString);
        fprintf (stderr, "Slow Control Load Error: %s\n", mysql_error (conn) );
        return 1;
    }

    //if ( mysql_warning_count (conn) > 0 )
    //{
    //    if (verbose) 
    //    {
    //        printf ("Warnings have occurred from the slow control event upload:\n");
    //        showWarnings (conn);
    //    }
    //}

    if (file_exists (outputFileName) )
    {
        remove (outputFileName);
    }

    sprintf (qryString, "SELECT value FROM SlowControl WHERE name='TARGPOS_CONTROL'");

    if ( mysql_query (conn, qryString) )
    {
        fprintf (stderr, "TARG_POSCONTROL Check Error: %s\n", mysql_error (conn) );
        return 1;
    }

    // Store the query result
    res1 = mysql_store_result (conn);
    // Grab the first (and only) row
    row = mysql_fetch_row (res1);

    if (mysql_num_rows (res1) > 0)
    {
        targPos = atoi (row[0]);
    }

    mysql_free_result (res1);

    sprintf (qryString, "SELECT value FROM SlowControl WHERE name='local_spillcount'");

    if ( mysql_query (conn, qryString) )
    {
        fprintf (stderr, "Local Spillcount Check Error: %s\n", mysql_error (conn) );
        return 1;
    }

    // Store the query result
    res4 = mysql_store_result (conn);
    // Grab the first (and only) row
    row = mysql_fetch_row (res4);

    if (mysql_num_rows (res4) > 0)
    {
        spillID = atoi (row[0]) + 1;
        if (verbose) 
        {
            printf ("SLOWCONTROL EVENT: spillID = %i (coda event %i)\n",
                (spillID-1), codaEventID );
        }
    }

    mysql_free_result (res4);

    /*
    if (runNum < 11074)
    {
        sprintf (qryString, "SELECT value FROM SlowControl WHERE name='F:NM3ION'");
    }
    else
    {
        sprintf (qryString, "SELECT value FROM SlowControl WHERE name='F:NM3SEM'");
    }

    if ( mysql_query (conn, qryString) )
    {
        fprintf (stderr, "F:NM3ION Check Error: %s\n", mysql_error (conn) );
        return 1;
    }
    */

    sprintf (qryString, "SELECT MIN(time) FROM SlowControl WHERE time>0");

    if ( mysql_query (conn, qryString) )
    {
        fprintf (stderr, "SlowTime Check Error: %s\n", mysql_error (conn) );
        return 1;
    }

    // Store the query result
    res2 = mysql_store_result (conn);
    // Grab the first (and only) row
    row = mysql_fetch_row (res2);

    if (mysql_num_rows (res2) > 0)
    {
        sprintf (qryString,
                 "UPDATE Spill SET time=\"%s\" WHERE spillID=%i", row[0], spillID-1);

        mysql_free_result(res2);

        if (mysql_query (conn, qryString) )
        {
            fprintf (stderr, "SlowTime Update Error: %s\n", mysql_error (conn) );
        }
    } else {
        mysql_free_result (res2);
    }

    // For spillcounter use -- not artificial spillID assignment
    sprintf (beamSQL, "INSERT INTO Beam SELECT %i, %i, name, value \n"
             "FROM SlowControl\n"
             "WHERE type='beam'", runNum, (spillID - 1) );
    sprintf (hvSQL, "INSERT INTO HV SELECT %i, %i, name, value \n"
             "FROM SlowControl \n"
             "WHERE type='Chamber_HV'", runNum, (spillID - 1) );
    sprintf (envSQL, "INSERT INTO Environment SELECT %i, %i, name, value \n"
             "FROM SlowControl \n"
             "WHERE type='Environment'", runNum, (spillID - 1) );
    sprintf (targ1SQL, "INSERT INTO Target SELECT %i, %i, name, value \n"
             "FROM SlowControl\n"
             "WHERE name LIKE 'FREQ%%' OR name LIKE 'PROX%%' OR name='TARGPOS_CONTROL' "
             "OR name LIKE 'TARG\\_%%'", runNum, (spillID) );
    sprintf (targ2SQL, "INSERT INTO Target SELECT %i, %i, name, value \n"
             "FROM SlowControl\n"
             "WHERE NOT(name LIKE 'FREQ%%' OR name LIKE 'PROX%%' OR name='TARGPOS_CONTROL' "
             "OR name LIKE 'TARG\\_%%') AND type='target'", runNum, (spillID - 1) );

    // Submit the query to the server
    if (mysql_query (conn, beamSQL) )
    {
        fprintf (stderr, "%s\n", beamSQL);
        fprintf (stderr, "Beam Insert Error: %s\n", mysql_error (conn) );
        return 1;
    }

    if (mysql_query (conn, hvSQL) )
    {
        fprintf (stderr, "%s\n", hvSQL);
        fprintf (stderr, "HV Insert Error: %s\n", mysql_error (conn) );
        return 1;
    }

    if (mysql_query (conn, envSQL) )
    {
        fprintf (stderr, "%s\n", envSQL);
        fprintf (stderr, "Environment Insert Error: %s\n", mysql_error (conn) );
        return 1;
    }

    if (mysql_query (conn, targ1SQL) )
    {
        fprintf (stderr, "%s\n", targ1SQL);
        fprintf (stderr, "Target 1 Insert Error: %s\n", mysql_error (conn) );
        return 1;
    }

    if (mysql_query (conn, targ2SQL) )
    {
        fprintf (stderr, "%s\n", targ2SQL);
        fprintf (stderr, "Target 2 Insert Error: %s\n", mysql_error (conn) );
        return 1;
    }

    if ( mysql_query (conn, "DELETE FROM SlowControl") )
    {
        fprintf (stderr, "SlowControl Delete Error: %s\n", mysql_error (conn) );
        return 1;
    }

    return 0;
}

int eventNewTDCSQL (int physicsEvent[70000], int j, RawHitStorage* hitStorage)
{
    //
    //	Returns: 0 if successful
    //		 1 on error
    //
    int windows = 0;
    int channel = 0;
    double Td, Ts, Tt;
    int val, k, l, m, n;
    double signalWidth[64];
    double tdcTime[64];
    int boardID = 0;
    int channelFired[64];
    int channelFiredBefore[64];
    // Word 1 contains boardID only
    boardID = get_hex_bits (physicsEvent[j], 7, 4);
    j++;
    // Word 2 contains Td and signal window width
    Td = get_hex_bits (physicsEvent[j], 3, 2);
    Td = Td * 2 * 10; // x2 (resolution) x10 (width of clock)

    if (get_bin_bit (physicsEvent[j], 1) )
    {
        windows = 64;
    }
    else
    {
        windows = 32;
    }

    // printf("#Windows: %i\n",windows);
    j++;
    // Word 3 contains Tt
    Tt = 0.0;

    for (k = 0; k < 3; k++)
    {
        val = get_hex_bit (physicsEvent[j], k);
        Tt += trigger_pattern[val];
    }

    j++;

    // Initialize the ridiculous constants required for reading channel information
    for (k = 0; k < 64; k++)
    {
        // This flags when a channel fires
        channelFired[k] = 0;
        // This flags when a channel has fired previously
        channelFiredBefore[k] = 0;
        // This keeps track of the signal width
        signalWidth[k] = 0;
    }

    // 8*windows of words to follow for each channel
    for (k = 0; k < windows; k++)
    {
        channel = 0;

        for (m = 0; m < 8; m++)
        {
            // If all channels are not fired, mark them as such
            //	If they were previously fired, register them as an INSERT row
            if (physicsEvent[j] == 0)
            {
                j++;

                for (n = 0; n < 8; n++)
                {
                    if (channelFiredBefore[channel])
                    {
                        hitStorage->insert_hit(spillID, eventID, hitStorage->curROCID,
                                               boardID, channel, tdcTime[channel]);
                        tdcTime[channel] = 0;
                        signalWidth[channel] = 0;
                    }

                    channelFired[channel] = 0;
                    channelFiredBefore[channel] = 0;
                    channel++;
                }

                // If there is a signal, store all pertinent information.
                //	If there's a signal and it's the last window, add the info to the INSERT
            }
            else
            {
                for (l = 0; l < 8; l++)
                {
                    val = get_hex_bit (physicsEvent[j], l);

                    if (val != 0)
                    {
                        channelFired[channel] = 1;
                    }

                    // If there is a signal, but there was no signal before, we have a new hit
                    if (channelFired[channel] && ! (channelFiredBefore[channel]) && (k < (windows - 1) ) )
                    {
                        // Compute TDC time
                        Ts = k * 10 + ts_pattern[val];
                        tdcTime[channel] = 1280 - Td - Ts + Tt;
                        // Establish first value of signal width
                        signalWidth[channel] = (10 - channel_pattern[val]);
                        // Mark that this channel has fired
                        channelFiredBefore[channel] = 1;
                    }
                    // If there is a signal, and it fired previously, simply add to the signal width
                    else if (channelFired[channel] && channelFiredBefore[channel] && (k < (windows - 1) ) )
                    {
                        // Add to signal width
                        signalWidth[channel] += (10 - channel_pattern[val]);
                        // Redundant, but keeps things straight
                        channelFiredBefore[channel] = 1;
                    }
                    // If a signal has fired, fired before, and it's the last window, make an INSERT row
                    else if (channelFired[channel] && channelFiredBefore[channel] && k == (windows - 1) )
                    {
                        signalWidth[channel] += (10 - channel_pattern[val]);
                        hitStorage->insert_hit(spillID, eventID, hitStorage->curROCID, boardID,
                                               channel, tdcTime[channel]);
                        
                        channelFired[channel] = 1;
                        channelFiredBefore[channel] = 1;
                        tdcTime[channel] = 0;
                        signalWidth[channel] = 0;
                    }
                    // If a signal fires, but has not fired before, and it is the last window,
                    //	calc TDC time, signal width, and make INSERT row
                    else if (channelFired[channel] && ! (channelFiredBefore[channel]) && k == (windows - 1) )
                    {
                        // Compute TDC time
                        Ts = k * 10 + ts_pattern[val];
                        tdcTime[channel] = 1280 - (Td + Ts) + Tt;
                        // Establish first value of signal width
                        signalWidth[channel] = (10 - channel_pattern[val]);
                        
                        hitStorage->insert_hit(spillID, eventID, hitStorage->curROCID, boardID,
                                               channel, tdcTime[channel]);

                        channelFiredBefore[channel] = 1;
                        tdcTime[channel] = 0;
                        signalWidth[channel] = 0;
                    }
                    // If it did not fire, and it fired previously, make an INSERT row
                    else if (! (channelFired[channel]) && channelFiredBefore[channel])
                    {
                        hitStorage->insert_hit(spillID, eventID, hitStorage->curROCID, boardID,
                                               channel, tdcTime[channel]);

                        channelFiredBefore[channel] = 0;
                        tdcTime[channel] = 0;
                        signalWidth[channel] = 0;
                    }

                    channel++;
                }

                j++;
            }
        }
    }

    return j;
}

int eventWCTDCSQL (int physicsEvent[70000], int j, RawHitStorage* hitStorage)
{
    //
    //	Returns: 0 if successful
    //		 1 on error
    //
    int windows = 0;
    int channel = 0;
    double Td, Ts, Tt;
    int val, k, l, m, n;
    double signalWidth[64];
    double tdcTime[64];
    int boardID = 0;
    int channelFired[64];
    int channelFiredBefore[64];
    
    // Word 1 contains boardID only
    boardID = get_hex_bits (physicsEvent[j], 7, 4);
    j++;
    // Word 2 contains Td and signal window width
    Td = get_hex_bits (physicsEvent[j], 3, 2);
    Td = Td * 40; // x40 (width of clock)

    if (get_bin_bit (physicsEvent[j], 1) )
    {
        windows = 64;
    }
    else
    {
        windows = 32;
    }

    // printf("#Windows: %i\n",windows);
    j++;
    // Word 3 contains Tt
    Tt = 0.0;

    for (k = 0; k < 3; k++)
    {
        val = get_hex_bit (physicsEvent[j], k);
        Tt += wc_trigger_pattern[val];
    }

    j++;

    // Initialize the ridiculous constants required for reading channel information
    for (k = 0; k < 64; k++)
    {
        // This flags when a channel fires
        channelFired[k] = 0;
        // This flags when a channel has fired previously
        channelFiredBefore[k] = 0;
        // This keeps track of the signal width
        signalWidth[k] = 0;
    }

    // 8*windows of words to follow for each channel
    for (k = 0; k < windows; k++)
    {
        channel = 0;

        for (m = 0; m < 8; m++)
        {
            // If all channels are not fired, mark them as such
            //	If they were previously fired, register them as an INSERT row
            if (physicsEvent[j] == 0)
            {
                j++;

                for (n = 0; n < 8; n++)
                {
                    if (channelFiredBefore[channel])
                    {
                        hitStorage->insert_hit(spillID, eventID, hitStorage->curROCID,
                                               boardID, channel, tdcTime[channel]);

                        tdcTime[channel] = 0;
                        signalWidth[channel] = 0;
                    }

                    channelFired[channel] = 0;
                    channelFiredBefore[channel] = 0;
                    channel++;
                }

                // If there is a signal, store all pertinent information.
                //	If there's a signal and it's the last window, add the info to the INSERT
            }
            else
            {
                for (l = 0; l < 8; l++)
                {
                    val = get_hex_bit (physicsEvent[j], l);

                    if (val != 0)
                    {
                        channelFired[channel] = 1;
                    }

                    // If there is a signal, but there was no signal before, we have a new hit
                    if (channelFired[channel] && ! (channelFiredBefore[channel]) && (k < (windows - 1) ) )
                    {
                        // Compute TDC time
                        Ts = k * 40 + wc_ts_pattern[val];
                        tdcTime[channel] = (1280 * 4) - Td - Ts + Tt;
                        // Establish first value of signal width
                        signalWidth[channel] = (40 - wc_channel_pattern[val]);
                        // Mark that this channel has fired
                        channelFiredBefore[channel] = 1;
                    }
                    // If there is a signal, and it fired previously, simply add to the signal width
                    else if (channelFired[channel] && channelFiredBefore[channel] && (k < (windows - 1) ) )
                    {
                        // Add to signal width
                        signalWidth[channel] += (40 - wc_channel_pattern[val]);
                        // Redundant, but keeps things straight
                        channelFiredBefore[channel] = 1;
                    }
                    // If a signal has fired, fired before, and it's the last window, make an INSERT row
                    else if (channelFired[channel] && channelFiredBefore[channel] && k == (windows - 1) )
                    {
                        signalWidth[channel] += (40 - wc_channel_pattern[val]);

                        hitStorage->insert_hit(spillID, eventID, hitStorage->curROCID, boardID,
                                               channel, tdcTime[channel]);

                        channelFired[channel] = 1;
                        channelFiredBefore[channel] = 1;
                        tdcTime[channel] = 0;
                        signalWidth[channel] = 0;
                    }
                    // If a signal fires, but has not fired before, and it is the last window,
                    //	calc TDC time, signal width, and make INSERT row
                    else if (channelFired[channel] && ! (channelFiredBefore[channel]) && k == (windows - 1) )
                    {
                        // Compute TDC time
                        Ts = k * 40 + wc_ts_pattern[val];
                        tdcTime[channel] = (1280 * 4) - (Td + Ts) + Tt;
                        // Establish first value of signal width
                        signalWidth[channel] = (40 - wc_channel_pattern[val]);
                        
                        hitStorage->insert_hit(spillID, eventID, hitStorage->curROCID, boardID,
                                               channel, tdcTime[channel]);

                        channelFiredBefore[channel] = 1;
                        tdcTime[channel] = 0;
                        signalWidth[channel] = 0;
                    }
                    // If it did not fire, and it fired previously, make an INSERT row
                    else if (! (channelFired[channel]) && channelFiredBefore[channel])
                    {
                        hitStorage->insert_hit(spillID, eventID, hitStorage->curROCID, boardID,
                                               channel, tdcTime[channel]);

                        channelFiredBefore[channel] = 0;
                        tdcTime[channel] = 0;
                        signalWidth[channel] = 0;
                    }

                    channel++;
                }

                j++;
            }
        }
    }

    return j;
}

int eventZSWCTDCSQL (int physicsEvent[70000], int j, RawHitStorage* hitStorage)
{
    //
    //	Returns: 0 if successful
    //		 1 on error
    //
    int windows, window, word, val, n;
    unsigned char channel;
    double Td, Ts, Tt;
    int previous_value[64];
    int previous_window[64];
    double tdc_time[64];
    double signal_width[64];
    int boardID;

    // Word 1 contains boardID only
    boardID = (get_hex_bits(physicsEvent[j],7,4));
    j++;
    // Word 2 contains Td and signal window width
    Td = (get_hex_bits (physicsEvent[j], 3, 2) ) * 40;

    if (get_bin_bit (physicsEvent[j], 1) )
    {
        windows = 64;
    }
    else
    {
        windows = 32;
    }

    j++;
    // Word 3 contains Tt
    Tt = 0.0;

    for (n = 0; n < 3; n++)
    {
        val = get_hex_bit (physicsEvent[j], n);
        Tt += wc_trigger_pattern[val];
    }

    j++;

    for (n = 0; n < 64; n++)
    {
        previous_value[n] = 15;
        previous_window[n] = -1;
        tdc_time[n] = 0.0;
        signal_width[n] = 0.0;
    }

    window = 0;
    word = 0;
    channel = 0;

    // 0xe906c0da is the 'end' word in this format
    while (physicsEvent[j] != (int)0xe906c0da)
    {
        if (get_hex_bits (physicsEvent[j], 7, 5) == 0xe906d)
        {
            word = (get_hex_bits (physicsEvent[j], 2, 3) );
            window = floor (word / 8.0);
            channel = (word % 8) * 8;

            for (n = 0; n < 64; n++)
            {
                if (tdc_time[n] > 0.0 && ( (previous_window[n] == window - 1 && n < channel) ||
                                           (previous_window[n] < (window - 1) ) ) )
                {
                    hitStorage->insert_hit(spillID, eventID, hitStorage->curROCID,
                                           boardID, n, tdc_time[n]);

                    tdc_time[n] = 0.0;
                    signal_width[n] = 0.0;
                }
            }

            j++;
        }
        else
        {
            for (n = 0; n < 8; n++)
            {
                val = get_hex_bit (physicsEvent[j], n);

                if (val != 0x0)
                {
                    // Definitive rising edge
                    if ( (channel_bit_pattern[val] & 8) == 0)
                    {
                        // If it fired in previous window with a XXX1 bit pattern,
                        // and it has a tdc time, submit its row
                        if ( (previous_window[channel] == window - 1) &&
                                (previous_value[channel] & 1) == 1 && tdc_time[channel] > 0.0)
                        {
                            hitStorage->insert_hit(spillID, eventID, hitStorage->curROCID, boardID,
                                                   channel, tdc_time[channel]);

                            tdc_time[channel] = 0.0;
                            signal_width[channel] = 0.0;
                        }

                        // Calculate and store the tdc time and signal width
                        Ts = window * 40 + wc_ts_pattern[val];
                        tdc_time[channel] = 5120 - Td - Ts + Tt;
                        signal_width[channel] = (40 - wc_channel_pattern[val]);
                        // Record its value and when it fired
                        previous_window[channel] = window;
                        previous_value[channel] = channel_bit_pattern[val];

                        // If this is the last window, record hit now
                        if (window == windows && tdc_time[channel] > 0.0)
                        {
                            hitStorage->insert_hit(spillID, eventID, hitStorage->curROCID, boardID,
                                                   channel, tdc_time[channel]);
                            tdc_time[channel] = 0.0;
                            signal_width[channel] = 0.0;
                        }

                        // Definitive falling edge
                    }
                    else if ( (channel_bit_pattern[val] & 1) == 0)
                    {
                        // If this is the rising and falling edge, set TDC time and signal width
                        if ( (previous_window[channel] < (window - 1) ) ||
                                (previous_window[channel] == (window - 1) && (previous_value[channel] & 1) == 0) )
                        {
                            Ts = window * 40 + wc_ts_pattern[val];
                            tdc_time[channel] = 5120 - Td - Ts + Tt;
                            signal_width[channel] = (40 - wc_channel_pattern[val]);
                        }
                        else
                        {
                            signal_width[channel] += (40 - wc_channel_pattern[val]);
                        }

                        // otherwise, this follows another signal
                        previous_window[channel] = window;
                        previous_value[channel] = channel_bit_pattern[val];

                        // For a signal that never had a rising edge, will have 0 tdcTime, shouldn't be recorded
                        if (tdc_time[channel] > 0.0)
                        {
                            hitStorage->insert_hit(spillID, eventID, hitStorage->curROCID, boardID,
                                                   channel, tdc_time[channel]);

                            tdc_time[channel] = 0.0;
                            signal_width[channel] = 0.0;
                        }

                        // If the signal is a 1111, then...
                    }
                    else if (channel_bit_pattern[val] == 15)
                    {
                        // If it fired previously, and it's a continued signal, add to signal width and move on
                        if (previous_window[channel] == (window - 1) && (previous_value[channel] & 1) == 1)
                        {
                            // Add to signal width
                            signal_width[channel] += (40 - wc_channel_pattern[val]);
                            // Record its value and when it fired
                            previous_window[channel] = window;
                            previous_value[channel] = channel_bit_pattern[val];
                        }
                        // Else if it fired before and it was a falling edge or if it's a new signal,
                        // start new signal
                        else if ( (previous_window[channel] < (window - 1) ) ||
                                  (previous_window[channel] == (window - 1) && (previous_value[channel] & 1) == 0) )
                        {
                            // Calculate and store the tdc time and signal width
                            Ts = window * 40 + wc_ts_pattern[val];
                            tdc_time[channel] = 5120 - Td - Ts + Tt;
                            signal_width[channel] = (40 - wc_channel_pattern[val]);
                            // Record its value and when it fired
                            previous_window[channel] = window;
                            previous_value[channel] = channel_bit_pattern[val];
                        }
                    }

                    // If the value is zero, see if it fired before but was not recorded
                }
                else
                {
                    if (tdc_time[channel] > 0.0)
                    {
                        hitStorage->insert_hit(spillID, eventID, hitStorage->curROCID, boardID,
                                               channel, tdc_time[channel]);

                        tdc_time[channel] = 0.0;
                        signal_width[channel] = 0.0;
                    }
                }

                channel++;
            }

            word++;

            if (floor (word / 8.0) > window)
            {
                window = floor (word / 8.0);
            }

            channel = (word % 8) * 8;
            j++;
        }
    }

    for (n = 0; n < 64; n++)
    {
        if (tdc_time[n] > 0.0)
        {
            hitStorage->insert_hit(spillID, eventID, hitStorage->curROCID,
                                   boardID, n, tdc_time[n]);
        }
    }

    j++;
    return j;
}

int eventv1495TDCSQL (MYSQL *conn, int physicsEvent[70000], int j, 
                      Event* event, RawHitStorage* hitStorage)
{
    //
    //	Returns: 0 if successful
    //		 1 on error
    //
    char qryString[1000];
    unsigned int numChannels = 0;
    unsigned int stopTime, channel, channelTime;
    double tdcTime;
    unsigned int error;
    unsigned int ignore = 0;
    unsigned int k = 0;
    int boardID;

    // Word 1 contains boardID only 0x0000xxxx
    boardID = get_hex_bits (physicsEvent[j], 3, 4);
    j++;
    // Check to see if there is an error in next word
    // If so, don't submit the information from this board for this event
    error = get_bin_bit (physicsEvent[j + 1], 12);

    if (error != 1)
    {
        ignore = 1;
    }

    hitStorage->all1495++;
    error = 0;

    if ( (physicsEvent[j] & 0xFFFF) == 0xD1AD)
    {
        hitStorage->d1ad1495++;
        error = error | 1;
    }

    if ( (physicsEvent[j + 1] & 0xFFFF) == 0xD2AD)
    {
        hitStorage->d2ad1495++;
        error = error | 2;
    }

    if ( (physicsEvent[j + 1] & 0xFFFF) == 0xD3AD)
    {
        hitStorage->d3ad1495++;
        error = error | 4;
    }

    if (error != 0)
    {
        event->dataQuality = event->dataQuality | 2;
        sprintf (qryString, "INSERT INTO TriggerHit (spillID, eventID, "
                 "detectorName, elementID, errorBits) VALUES "
                 "(%i, %i, \"%i\", %i, %i)",
                 spillID, eventID, hitStorage->curROCID, boardID, error);

        if (mysql_query (conn, qryString) )
        {
            fprintf (stderr, "0xD?AD Logging Error: %s\n", mysql_error (conn) );
            return -1;
        }
    }
    else
    {
        hitStorage->good1495++;
    }

    // Contains 0x000000xx where xx is number of channel entries
    numChannels = (physicsEvent[j] & 0x0000FFFF);

    if (numChannels > 255)
    {
        //printf("number of Channels stated in v1495 readout exceeds 255. Skipping...\n");
        return j + 2;
    }

    j++;
    // Next contains 0x00001xxx where xxx is the stop time
    stopTime = (physicsEvent[j] & 0x00000FFF);
    j++;
    k = 0;

    // 0x0000xxyy where xx is the channel and yy is the channel time
    while (k < numChannels)
    {
        channel = get_hex_bits (physicsEvent[j], 3, 2);
        channelTime = get_hex_bits (physicsEvent[j], 1, 2);
        tdcTime = (stopTime - channelTime);

        if (!ignore)
        {
            hitStorage->insert_trigger_hit(spillID, eventID, hitStorage->curROCID,
                                           boardID, channel, tdcTime);
        }
        
        j++;
        k++;
    }
    return j;
}

int eventReimerTDCSQL (int physicsEvent[70000], int j, RawHitStorage* hitStorage)
{
    //
    //	Returns: 0 if successful
    //		 1 on error
    //
    int windows, window, word, val, n;
    unsigned char channel;
    double Td, Ts, Tt;
    int previous_value[64];
    int previous_window[64];
    double tdc_time[64];
    double signal_width[64];
    int boardID;
    
    // Word 1 contains boardID only
    boardID = (get_hex_bits(physicsEvent[j],7,4));
    // printf("boardID: %.8x\n",boardID);
    j++;
    // Word 2 contains Td and signal window width
    // x2 (resolution) x10 (width of clock)
    Td = (get_hex_bits (physicsEvent[j], 3, 2) ) * 2 * 10;

    if (get_bin_bit (physicsEvent[j], 1) )
    {
        windows = 64;
    }
    else
    {
        windows = 32;
    }

    j++;
    // Word 3 contains Tt
    Tt = 0.0;

    for (n = 0; n < 3; n++)
    {
        val = get_hex_bit (physicsEvent[j], n);
        Tt += trigger_pattern[val];
    }

    j++;

    for (n = 0; n < 64; n++)
    {
        previous_value[n] = 15;
        previous_window[n] = -1;
        tdc_time[n] = 0.0;
        signal_width[n] = 0.0;
    }

    window = 0;
    word = 0;
    channel = 0;

    // 0xe906c0da is the 'end' word in this format
    while (physicsEvent[j] != (int)0xe906c0da)
    {
        if (get_hex_bits (physicsEvent[j], 7, 5) == 0xe906d)
        {
            word = (get_hex_bits (physicsEvent[j], 2, 3) );
            window = floor (word / 8.0);
            channel = (word % 8) * 8;

            for (n = 0; n < 64; n++)
            {
                if (tdc_time[n] > 0.0 && ( (previous_window[n] == window - 1 && n < channel) ||
                                           (previous_window[n] < (window - 1) ) ) )
                {
                    hitStorage->insert_hit(spillID, eventID, hitStorage->curROCID,
                                           boardID, n, tdc_time[n]);

                    tdc_time[n] = 0.0;
                    signal_width[n] = 0.0;
                }
            }

            j++;
        }
        else
        {
            for (n = 0; n < 8; n++)
            {
                val = get_hex_bit (physicsEvent[j], n);

                if (val != 0x0)
                {
                    // Definitive rising edge
                    if ( (channel_bit_pattern[val] & 8) == 0)
                    {
                        // If it fired in previous window with a XXX1 bit pattern, and it has a tdc time,
                        // submit its row
                        if ( (previous_window[channel] == window - 1) &&
                                (previous_value[channel] & 1) == 1 && tdc_time[channel] > 0.0)
                        {
                            hitStorage->insert_hit(spillID, eventID, hitStorage->curROCID, boardID,
                                                   channel, tdc_time[channel]);

                            tdc_time[channel] = 0.0;
                            signal_width[channel] = 0.0;
                        }

                        // Calculate and store the tdc time and signal width
                        Ts = window * 10 + ts_pattern[val];
                        tdc_time[channel] = 1280 - Td - Ts + Tt;
                        signal_width[channel] = (10 - channel_pattern[val]);
                        // Record its value and when it fired
                        previous_window[channel] = window;
                        previous_value[channel] = channel_bit_pattern[val];

                        // If this is the last window, record hit now
                        if (window == windows && tdc_time[channel] > 0.0)
                        {
                            hitStorage->insert_hit(spillID, eventID, hitStorage->curROCID, boardID,
                                                   channel, tdc_time[channel]);

                            tdc_time[channel] = 0.0;
                            signal_width[channel] = 0.0;
                        }

                        // Definitive falling edge
                    }
                    else if ( (channel_bit_pattern[val] & 1) == 0)
                    {
                        // If this is the rising and falling edge, set TDC time and signal width
                        if ( (previous_window[channel] < (window - 1) ) ||
                                (previous_window[channel] == (window - 1) && (previous_value[channel] & 1) == 0) )
                        {
                            Ts = window * 10 + ts_pattern[val];
                            tdc_time[channel] = 1280 - Td - Ts + Tt;
                            signal_width[channel] = (10 - channel_pattern[val]);
                        }
                        else
                        {
                            signal_width[channel] += (10 - channel_pattern[val]);
                        }

                        // otherwise, this follows another signal
                        previous_window[channel] = window;
                        previous_value[channel] = channel_bit_pattern[val];

                        // For a signal that never had a rising edge, will have 0 tdc_time, shouldn't be recorded
                        if (tdc_time[channel] > 0.0)
                        {
                            hitStorage->insert_hit(spillID, eventID, hitStorage->curROCID, boardID,
                                                   channel, tdc_time[channel]);

                            tdc_time[channel] = 0.0;
                            signal_width[channel] = 0.0;
                        }

                        // If the signal is a 1111, then...
                    }
                    else if (channel_bit_pattern[val] == 15)
                    {
                        // If it fired previously, and it's a continued signal, add to signal width and move on
                        if (previous_window[channel] == (window - 1) && (previous_value[channel] & 1) == 1)
                        {
                            // Add to signal width
                            signal_width[channel] += (10 - channel_pattern[val]);
                            // Record its value and when it fired
                            previous_window[channel] = window;
                            previous_value[channel] = channel_bit_pattern[val];
                        }
                        // Else if it fired before and it was a falling edge or if it's a new signal, start new signal
                        else if ( (previous_window[channel] < (window - 1) ) ||
                                  (previous_window[channel] == (window - 1) && (previous_value[channel] & 1) == 0) )
                        {
                            // Calculate and store the tdc time and signal width
                            Ts = window * 10 + ts_pattern[val];
                            tdc_time[channel] = 1280 - Td - Ts + Tt;
                            signal_width[channel] = (10 - channel_pattern[val]);
                            // Record its value and when it fired
                            previous_window[channel] = window;
                            previous_value[channel] = channel_bit_pattern[val];
                        }
                    }

                    // If the value is zero, see if it fired before but was not recorded
                }
                else
                {
                    if (tdc_time[channel] > 0.0)
                    {
                        hitStorage->insert_hit(spillID, eventID, hitStorage->curROCID, boardID,
                                               channel, tdc_time[channel]);

                        tdc_time[channel] = 0.0;
                        signal_width[channel] = 0.0;
                    }
                }

                channel++;
            }

            word++;

            if (floor (word / 8.0) > window)
            {
                window = floor (word / 8.0);
            }

            channel = (word % 8) * 8;
            j++;
        }
    }

    for (n = 0; n < 64; n++)
    {
        if (tdc_time[n] > 0.0)
        {
            hitStorage->insert_hit(spillID, eventID, hitStorage->curROCID,
                                   boardID, n, tdc_time[n]);
        }
    }

    j++;
    return j;
}

int eventJyTDCSQL (int physicsEvent[70000], int j, RawHitStorage* hitStorage)
{
    int windows = 0;
    int channel = 0;
    double Td, Ts, Tt;
    int val;
    double tdcTime;
    int window;
    int word, zsword;
    int boardID;
    int k;
    int buf = 0;
    int channelFiredBefore[64];
    
    // Word 1 contains boardID only
    boardID = get_hex_bits (physicsEvent[j], 7, 4);
    j++;
    // Word 2 contains Td and signal window width
    // x2 (resolution) x10 (width of clock)
    Td = (get_hex_bits (physicsEvent[j], 3, 2) ) * 2 * 10;

    if (get_bin_bit (physicsEvent[j], 1) )
    {
        windows = 64;
    }
    else
    {
        windows = 32;
    }

    j++;
    buf = (0xFFFF & physicsEvent[j]);
    j++;
    // Word 3 contains Tt
    Tt = 0.0;

    for (k = 0; k < 3; k++)
    {
        val = get_hex_bit (physicsEvent[j], k);
        Tt += trigger_pattern[val];
    }

    j++;

    for (k = 0; k < 64; k++)
    {
        // This flags the window in which a channel has fired previously
        channelFiredBefore[k] = 0xBD;
    }

    window = 0;
    zsword = 2;
    word = 0;
    channel = 0;
    word = 0;

    while (zsword <= buf)
    {
        if ( (physicsEvent[j] & 0xFF000000) == 0x88000000)
        {
            for (k = 1; k < (buf - zsword) + 1; k++)
            {
                // Look for the next 0x88 word, see where the next set
                // of zeroes starts, and count backwards from there
                if ( (physicsEvent[j + k] & 0xFF000000) == 0x88000000)
                {
                    word = (physicsEvent[j + k] & 0x00000FFF) - (k - 1);
                    // Set k to exit the for loop
                    k = buf;
                }
                else if (zsword + k == buf)
                {
                    word = (windows * 8) - (k - 1);
                }
            }

            // Figure out which window and channel to start with
            window = floor (word / 8.0);
            channel = (word % 8) * 8;
        }
        else
        {
            for (k = 0; k < 8; k++)
            {
                val = get_hex_bit (physicsEvent[j], k);

                if (val != 0x0)
                {
                    if (window != channelFiredBefore[channel] + 1)
                    {
                        channelFiredBefore[channel] = window;
                        Ts = window * 10 + ts_pattern[val];
                        tdcTime = 1280 - Td - Ts + Tt;
                        
                        hitStorage->insert_hit(spillID, eventID, hitStorage->curROCID,
                                               boardID, channel, tdcTime);
                    }

                    channelFiredBefore[channel] = window;
                }

                channel++;
            }

            word++;
            window = floor (word / 8.0);
            channel = (word % 8) * 8;
        }

        j++;
        zsword++;
    }

    return j;
}

int eventJyTDC2SQL (int physicsEvent[70000], int j, RawHitStorage* hitStorage)
{
    //
    //	Returns: 0 if successful
    //		 1 on error
    //
    int channel, tdcChannel, cableID;
    int fallChannel, fallCableID, fallTdcChannel;
    double tdcTime;
    double triggerTime, riseDataTime;
    //double fallDataTime;
    int nWords, rising, match;
    double triggerFine, dataFine, triggerRough, dataRough;
    int trigger_clock_cycle, data_clock_cycle;
    int boardID;
    int i = 0;

    // Word 2 containst boardID and number of words in buffer
    boardID = get_hex_bits (physicsEvent[j], 7, 4);
    nWords  = get_hex_bits (physicsEvent[j], 3, 4);
    j++;

    if (nWords == 1)
    {
        // No data for this board, move on.
        //j++;
    }
    else
    {
        // Dummy word *could* exist. Ugh. : 0xe906e906
        if (physicsEvent[j] == (int)0xe906e906)
        {
            j++;
        }

        // This is the trigger word, containing the trigger time
        trigger_clock_cycle = get_hex_bit (physicsEvent[j], 0);
        triggerRough = 4.0 * get_hex_bits (physicsEvent[j], 3, 3);

        if (get_bin_bit (physicsEvent[j], 16) )
        {
            triggerFine = 4.0 - (trigger_clock_cycle * (4.0 / 9.0) );
        }
        else
        {
            triggerFine = 0.5 * trigger_clock_cycle;
        }

        triggerTime = triggerRough + triggerFine;
        // nHits = get_bin_bits((physicsEvent[j] & 0x7FF00000),30,11); // Not used
        //printf("clock_cycle: %i, rough: %f, fine: %f, triggerTime: %f, nHits: %i\n",
        //	clock_cycle, rough, fine, triggerTime, nHits);

        while (nWords > 0)
        {
            rising = get_bin_bit (physicsEvent[j], 16);

            if (rising && (physicsEvent[j] & 0x80000000) == 0)
            {
                // Record the informatiom for this rising edge
                data_clock_cycle = get_hex_bit (physicsEvent[j], 0);
                dataFine = 4.0 - (data_clock_cycle * (4.0 / 9.0) );
                dataRough = 4.0 * get_hex_bits (physicsEvent[j], 3, 3);
                riseDataTime = dataRough + dataFine;
                tdcChannel = get_hex_bit (physicsEvent[j], 6);
                cableID = get_bin_bits (physicsEvent[j], 29, 2);
                channel = tdcChannel + (cableID * 16);
                tdcTime = triggerTime - riseDataTime;

                if (tdcTime < 0)
                {
                    tdcTime = triggerTime + (4096 - riseDataTime);
                }

                // Now, look for a falling edge that follows
                i = 1;
                match = 0;

                while (i < nWords && !match)
                {
                    rising = get_bin_bit (physicsEvent[j + i], 16);
                    fallTdcChannel = get_hex_bit (physicsEvent[j + i], 6);
                    fallCableID = get_bin_bits (physicsEvent[j + i], 29, 2);
                    fallChannel = fallTdcChannel + (16 * fallCableID);

                    if (fallChannel == channel && !rising)
                    {
                        // We've found this signal's falling edge
                        data_clock_cycle = get_hex_bit (physicsEvent[j + i], 0);
                        dataFine = 0.5 * data_clock_cycle;
                        dataRough = 4.0 * get_hex_bits (physicsEvent[j + i], 3, 3);
                        //fallDataTime = dataRough + dataFine;
                        //tdcSignalWidth[tdcCount] = fallDataTime - riseDataTime;

                        //if (tdcSignalWidth[tdcCount] < 0) {
                        //tdcSignalWidth[tdcCount] = fallDataTime + (4096 - riseDataTime);
                        //}

                        match = 1;
                    }
                    else if (fallChannel == channel && rising)
                    {
                        // We've run into the next rising edge of this channel
                        // Do nothing
                        match = 1;
                    }
                    else
                    {
                        match = 0;
                        i++;
                    }
                }
                hitStorage->insert_hit(spillID, eventID, hitStorage->curROCID,
                                       boardID, channel, tdcTime);
            }

            nWords--;
            j++;
        }
    }

    return j;
}

int eventScalerSQL (int physicsEvent[70000], int j, RawScalerStorage* scalerStorage)
{
    int i;
    int boardID;
    
    boardID = (0xFFFF & physicsEvent[j]);
    j++;

    for (i = 0; i < 32; i++)
    {
        scalerStorage->insert_scaler(spillID, codaEventID, scalerStorage->spillType,
                                     scalerStorage->curROCID, boardID, i, physicsEvent[j]);
        j++;
    }

    return j;
}

int eventTriggerSQL (int physicsEvent[70000], int j, Event* e)
{
    int triggerBits;
    int NIM[5];
    int MATRIX[5];
    int trigger[10];
    
    triggerBits = physicsEvent[j];
    rawEvent->setTriggerBits(triggerBits);

    totalTriggerBits = totalTriggerBits | (4095 & triggerBits);

    for (int i = 0; i < 10; i++)
    {
        trigger[i] = get_bin_bit (triggerBits, i);
    }

    if (runNum >= 4923)
    {
        NIM[0] = trigger[5];
        NIM[1] = trigger[6];
        NIM[2] = trigger[7];
        NIM[3] = trigger[8];
        NIM[4] = trigger[9];
        MATRIX[0] = trigger[0];
        MATRIX[1] = trigger[1];
        MATRIX[2] = trigger[2];
        MATRIX[3] = trigger[3];
        MATRIX[4] = trigger[4];
    }
    else
    {
        NIM[0] = trigger[0];
        NIM[1] = trigger[1];
        NIM[2] = trigger[2];
        NIM[3] = trigger[3];
        NIM[4] = trigger[4];
        MATRIX[0] = trigger[5];
        MATRIX[1] = trigger[6];
        MATRIX[2] = trigger[7];
        MATRIX[3] = trigger[8];
        MATRIX[4] = trigger[9];
    }
    
    for (int i = 0; i < 5; i++)
    {
        e->NIM[i] = NIM[i];
        e->MATRIX[i] = MATRIX[i];
    }

    j++;
    return j;
}

int eventTriggerCountSQL (int physicsEvent[70000], int j, Event* e)
{
    // Put the next value into the Event table
    if (get_hex_bits (physicsEvent[1], 7, 4) == 14)
    {
        e->RawMATRIX[0] = physicsEvent[j];
        j++;
        e->RawMATRIX[1] = physicsEvent[j];
        j++;
        e->RawMATRIX[2] = physicsEvent[j];
        j++;
        e->RawMATRIX[3] = physicsEvent[j];
        j++;
        e->RawMATRIX[4] = physicsEvent[j];
        j++;
        e->AfterInhMATRIX[0] = physicsEvent[j];
        j++;
        e->AfterInhMATRIX[1] = physicsEvent[j];
        j++;
        e->AfterInhMATRIX[2] = physicsEvent[j];
        j++;
        e->AfterInhMATRIX[3] = physicsEvent[j];
        j++;
        e->AfterInhMATRIX[4] = physicsEvent[j];
        j++;
    }
    else
    {
        for (int i = 0 ; i<5 ; i++)
        {
            e->RawMATRIX[i] = 0;
            e->AfterInhMATRIX[i] = 0;
        }
    }

    return j;
}
int physicsEventType (int physicsEvent[70000])
{
    return get_hex_bits (physicsEvent[1], 7, 4);
}

int eventSQL (MYSQL *conn, int physicsEvent[70000], DocoptArgs args,
              RawEventStorage* eventStorage, RawScalerStorage* scalerStorage,
              RawHitStorage* hitStorage)
{
    int i, j, v, x, number, submit;
    char qryString[1000];
    char spillNum[12];
    int evLength = 0; 
    int rocEvLength = 0; 
    int eventType = 0;
    int codaEvVmeTime = 0;
    int vmeTime = 0;
    int e906flag = 0;
    int ROCID;

    Event* e = new Event();

    submit = 0;
    evLength = physicsEvent[0];
    //eventCode = physicsEvent[1];
    //headerLength = physicsEvent[2];
    //headerCode = physicsEvent[3];
    //codaEventID = physicsEvent[4]+4;
    eventType = physicsEvent[5];
    // physicsEvent[6] == 0 (reserved value)

    switch (get_hex_bits (physicsEvent[1], 7, 4) )
    {
    case STANDARD_PHYSICS:
        j = 7;
        
        if (atoi(args.sampling) > 1){
            if (sampling_count < atoi(args.sampling))
            {
                sampling_count++;
                eventID++;
                return 0;
            }
            else if (sampling_count == atoi(args.sampling))
            {
                sampling_count = 1;
            }
        }
        eventID++;

        while (j < evLength)
        {
            rocEvLength = physicsEvent[j];
            v = j + rocEvLength;

            if (v > evLength)
            {
                fprintf (stderr, "ERROR: ROC Event Length exceeds event length\n"
                        "Event: %i, EventLength = %i, position = %i, rocEvLength = %.8x\n",
                        codaEventID, evLength, j, physicsEvent[j]);
                return 1;
            }

            // Variable v denotes the end of this ROC entry
            j++;
            ROCID = get_hex_bits (physicsEvent[j], 5, 2);
            hitStorage->curROCID = ROCID;
            scalerStorage->curROCID = ROCID;
            j++;
            // physicsEvent[j]=0x11111111; IGNORE
            j++;
            // physicsEvent[j]=0x11111111; IGNORE
            j++;
            vmeTime = physicsEvent[j];

            if (ROCID == 2)
            {
                codaEvVmeTime = vmeTime;
            }

            j++;
            e906flag = physicsEvent[j];
            j++;

            if ( rocEvLength < 5 )
            {
                j = v + 1;
            }

            while (j <= v)
            {
                j = format (conn, physicsEvent, j, v, e906flag, e,
                            hitStorage, scalerStorage);
                if ((j-1) < v)
                {
                    e906flag = physicsEvent[j-1];
                }

                if (j == -1)
                {
                    return 1;
                }
            }

            submit = 1;
        }

        break;

    case SLOW_CONTROL:

        // Slow Control Type
        if ( eventSlowSQL (conn, physicsEvent, args.dir) )
        {
            fprintf (stderr, "Slow Control Event Processing Error\n");
            return 1;
        }

        submit = 0;
        break;

    case PRESTART_INFO:

        // Slow Control Type
        if ( prestartInfo (conn, physicsEvent, args.dir) )
        {
            fprintf (stderr, "Prestart Info Event Processing Error\n");
            return 1;
        }

        submit = 0;
        break;

    case IGNORE_ME:
        // Dummy events inserted in order to flush out the buffer.
        // Ignore them.
        submit = 0;
        break;

    case SPILL_COUNTER:
        // Spill count type, contains current spillID
        strcpy(spillNum,"");

        for (x = 4; x < (evLength + 1); x++)
        {
            number = physicsEvent[x];

            for (i = 0; i < 4; i++)
            {
                sprintf (spillNum, "%s%c", spillNum, (number & 0xFF) );
                number = number >> 8;
            }
        }

        spillID = atoi (spillNum) + 1;
        if (verbose)
        {
            printf ("SPILL COUNTER EVENT: spillID = %i (coda event %i)\n",
                (spillID-1), codaEventID );
        }

        if ( submit_all_data(conn, args, eventStorage, hitStorage) )
        {
            fprintf (stderr, "Error submitting data. Exiting...\n");
            return 1;
        }

        submit = 0;
        break;

    case RUN_DESCRIPTOR:

        // Start of run descriptor
        if ( eventRunDescriptorSQL (conn, physicsEvent) )
        {
            fprintf (stderr, "Start of Run Descriptor Event Processing Error\n");
            return 1;
        }

        submit = 0;
        break;

    case BEGIN_SPILL:
        // Beginning of spill
        j = 7;
        //spillID++;
        if (verbose)
        {
            printf ("BEGIN SPILL EVENT, NEW SPILL spillID=%i (coda event %i)\n",
                    spillID, codaEventID);
        }
        scalerStorage->spillType = 1;

        while ( (j < evLength) && (physicsEvent[j] & 0xfffff000) != 0xe906f000)
        {
            rocEvLength = physicsEvent[j];
            v = j + rocEvLength;

            if ( (rocEvLength + j) > evLength)
            {
                fprintf (stderr, "This error: rocEvLength = %i, j = %i, evLength = %i\n",
                        rocEvLength, j, evLength);
                return 1;
            }

            j++;
            ROCID = get_hex_bits (physicsEvent[j], 7, 4);
            hitStorage->curROCID = ROCID;
            scalerStorage->curROCID = ROCID;
            j++;
            //GARBAGE = physicsEvent[9]; (3 words to follow)
            j++;
            //latchFlag = physicsEvent[11] = 1; (1 word to follow)
            j++;
            codaEvVmeTime = physicsEvent[j];
            j++;

            if (ROCID == 2)
            {
                sprintf (qryString, "INSERT INTO Spill "
                         "(spillID, runID, BOScodaEventID, targetPos, BOSvmeTime) "
                         "VALUES (%d,%d,%d,%d,%d)",
                         spillID, runNum, codaEventID, targPos, codaEvVmeTime);
                if ( mysql_query (conn, qryString) )
                {
                    fprintf (stderr, "Spill Insert Error: %s\n", mysql_error (conn) );
                    return 1;
                }
            }

            e906flag = physicsEvent[j];
            j++;

            if ( rocEvLength < 5 )
            {
                j = v + 1;
            }

            while (j <= v)
            {
                j = format (conn, physicsEvent, j, v, e906flag, e,
                            hitStorage, scalerStorage);
                if ((j-1) < v)
                {
                    e906flag = physicsEvent[j-1];
                }

                if (j == -1)
                {
                    return 1;
                }
            }

            if (scalerStorage->scalerCount > 0)
            {
                submit_scaler_data (args, scalerStorage);
            }
        }

        submit = 0;
        break;

    case END_SPILL:
        // End of spill.
        j = 7;
        scalerStorage->spillType = 2;
        if (verbose)
        {
            printf ("END SPILL EVENT, spillID=%i (coda event %i)\n", spillID, codaEventID );
        }

        while ( (j < evLength) && (physicsEvent[j] & 0xfffff000) != 0xe906f000)
        {
            rocEvLength = physicsEvent[j];
            v = j + rocEvLength;

            if ( (rocEvLength + j) > evLength)
            {
                fprintf (stderr, "This error: rocEvLength = %i, j = %i, evLength = %i\n",
                        rocEvLength, j, evLength);
                return 1;
            }

            j++;
            ROCID = get_hex_bits (physicsEvent[j], 7, 4);
            hitStorage->curROCID = ROCID;
            scalerStorage->curROCID = ROCID;
            j++;
            //GARBAGE = physicsEvent[9]; (3 words to follow)
            j++;
            //latchFlag = physicsEvent[11] = 1; (1 word to follow)
            j++;
            codaEvVmeTime = physicsEvent[j];
            j++;

            if (ROCID == 2)
            {
                sprintf (qryString, "UPDATE Spill SET EOScodaEventID=%i, EOSvmeTime=%i "
                         "WHERE spillID=%i", codaEventID, codaEvVmeTime, spillID);

                if ( mysql_query (conn, qryString) )
                {
                    fprintf (stderr, "EOS Update Table Error: %s\n", mysql_error (conn) );
                    return 1;
                }
            }

            e906flag = physicsEvent[j];
            j++;

            if ( rocEvLength < 5 )
            {
                j = v + 1;
            }

            while (j <= v)
            {
                j = format (conn, physicsEvent, j, v, e906flag, e,
                            hitStorage, scalerStorage);
                if ((j-1) < v)
                {
                    e906flag = physicsEvent[j-1];
                }

                if (j == -1)
                {
                    return 1;
                }
            }

            if (scalerStorage->scalerCount > 0)
            {
                submit_scaler_data (args, scalerStorage);
            }
        }

        if ( submit_all_data(conn, args, eventStorage, hitStorage) )
        {
            fprintf (stderr, "Error submitting data. Exiting...\n");
            return 1;
        }

        submit = 0;
        break;

    default:
        // Awaiting further event types
        fprintf (stderr, "Unknown event type: %x\n", eventType);
        submit = 0;
        break;
    }

    if (submit)
    {

        e->runID = runNum;
        e->spillID = spillID;
        e->eventID = eventID;
        e->codaEventID = codaEventID;
        e->vmeTime = codaEvVmeTime;
        eventStorage->insert_event(e);

        if(e->qieFlag==0){ e->dataQuality = e->dataQuality | 4; }

        // ROOT FILE HANDLING
        rawEvent->setEventInfo(runNum, spillID, eventID);
        rawEvent->setTargetPos(targPos);

        assert(rawEventBuffer.find(eventID) == rawEventBuffer.end());
        rawEventBuffer.insert(map<int, SRawEvent>::value_type(eventID, *rawEvent));

        rawEvent->clear();
        // ------------------

    }

    return 0;
}

int format (MYSQL *conn, int physicsEvent[70000], int j, int v, int e906flag,
            Event* event, RawHitStorage* hitStorage, RawScalerStorage* scalerStorage)
{
    // j is the current index position within physicsEvent
    // v is the last index in physicsEvent for this ROC

    if ( e906flag == (int)0xe906f002 )
    {
        // TDC event type -- HISTORICAL
        //j = eventTDCSQL(conn, physicsEvent, j);
        // Ignore Board
        while ( (j <= v) && ( (physicsEvent[j] & (int)0xFFFFF000) != (int)0xE906F000) )
        {
            j++;
        }

        if (j == v)
        {
            // The end of the ROC data has been reached, move on
            j++;
        }
    }
    else if ( e906flag == (int)0xE906F001 )
    {
        // Latch event type
        // DEPRICATED
        printf("LATCH NO LONGER SUPPORTED!");
        return -1;
    }
    else if ( e906flag == (int)0xE906F003 )
    {
        // Scaler event type
        j = eventScalerSQL (physicsEvent, j, scalerStorage);
    }
    else if ( e906flag == (int)0xE906F004 )
    {
        // New TDC (non zero-suppressed) event type
        j = eventNewTDCSQL (physicsEvent, j, hitStorage);
    }
    else if ( e906flag == (int)0xE906F005 )
    {
        // v1495 TDC type
        j = eventv1495TDCSQL (conn, physicsEvent, j, event, hitStorage);
    }
    else if ( e906flag == (int)0xE906F006 )
    {
        // Reimer TDC type
        j = eventReimerTDCSQL (physicsEvent, j, hitStorage);
    }
    else if ( e906flag == (int)0xE906F007 )
    {
        // Wide-Channel TDC type
        j = eventWCTDCSQL (physicsEvent, j, hitStorage);
    }
    else if ( e906flag == (int)0xE906F008 )
    {
        // Zero-Suppressed Wide-Channel TDC type
        j = eventZSWCTDCSQL (physicsEvent, j, hitStorage);
    }
    else if ( e906flag == (int)0xE906F009 )
    {
        // Zero-Suppressed Jy TDC type
        j = eventJyTDCSQL (physicsEvent, j, hitStorage);
    }
    else if ( e906flag == (int)0xE906F010 )
    {
        // Zero-Suppressed Jy TDC2 type
        j = eventJyTDC2SQL (physicsEvent, j, hitStorage);
    }
    else if ( e906flag == (int)0xE906F013 )
    {
        // Cherenkov info
        j = feeQIESQL (physicsEvent, j, event);
    }
    else if ( e906flag == (int)0xE906F014 )
    {
        // TriggerCount Info
        j = eventTriggerCountSQL (physicsEvent, j, event);
    }
    else if ( e906flag == (int)0xE906F020 )
    {
        // PPC Timer info. Ignore.
        j = j + 2;
    }
    else if ( e906flag == (int)0xE906F021 )
    {
        // PPC Timer info. Ignore.
        j = j + 2;
    }
    else if ( e906flag == (int)0xE906F00F )
    {
        // Trigger type
        j = eventTriggerSQL (physicsEvent, j, event);
    }
    else if ( e906flag == (int)0xE906F000 )
    {
        // Ignore Board
        while ( (j <= v) && ( (physicsEvent[j] & (int)0xFFFFF000) != (int)0xE906F000) )
        {
            j++;
        }

        if (j == v)
        {
            // The end of the ROC data has been reached, move on
            j++;
        }
    }
    else
    {
        fprintf (stderr, "Unexpected flag in CODA Event %i, ROC %i\n"
                "e906flag = physicsEvent[%i] == %.8x\n\n",
                codaEventID, hitStorage->curROCID, j, physicsEvent[j - 1]);
        return -1;
    }

    // Pass the error forward
    if (j == -1) return -1;

    if (physicsEvent[j] == (int)0xe906c0da)
    {
        j++;
    }

    if (j < v)
    {
        j++;
    }
    
    return j;
}

int endEventSQL (int physicsEvent[70000])
{
    // ================================================================
    //
    // Handles End Event CODA Events.
    // Returns: 0
   
    // Nothing really to use here. See these comments for the contents

    // int numEventsInRunAtEndOfRun = 0;
    // int endEventTime = 0;

    // int evLength = 0;
    // int endEventCode = 0;
    // evLength = physicsEvent[0];
    // endEventCode = physicsEvent[1];
    // endEventTime = physicsEvent[2];
    // physicsEvent[3] = 0 is a reserved value;
    // numEventsInRunAtEndOfRun = physicsEvent [4];
    return 0;
}


int submit_tdc_data (DocoptArgs args, RawHitStorage* hitStorage)
{

    uploadTDC(args.dir, args.server, args.schema, hitID,
              runNum, spillID, hitStorage, args.nohit);

    hitID += hitStorage->tdcCount;
    hitID += hitStorage->v1495Count;
    hitStorage->clear();

    return 0;
}

int submit_scaler_data (DocoptArgs args, RawScalerStorage* scalerStorage)
{

    uploadScaler(args.dir, args.server, args.schema, hitID, scalerStorage);

    hitID += scalerStorage->scalerCount;
    scalerStorage->clear_scaler();
    return 0;

}

int submit_all_data (MYSQL *conn, DocoptArgs args, RawEventStorage* eventStorage,
                     RawHitStorage* hitStorage)
{

    int write_to_root = 0;

    if (eventStorage->eventCount > 0)
    {
       eventStorage->submit(conn);
    }

    if ((hitStorage->tdcCount > 0) || (hitStorage->v1495Count > 0))
    {
        submit_tdc_data (args, hitStorage);
        write_to_root = 1;
    }

    if (write_to_root) {

        for(std::map<int, SRawEvent>::iterator iter = rawEventBuffer.begin(); 
            iter != rawEventBuffer.end(); ++iter)
        {
            *rawEvent = iter->second;
            rawEvent->reIndex(true);

            saveTree->Fill();
            rawEvent->clear();
        }

        saveFile->cd();
        saveTree->AutoSave("SaveSelf");
        // saveTree->Write("",TObject::kOverwrite);  // May cause a 'bus error' issue
        rawEventBuffer.clear();

    }

    return 0;
}
