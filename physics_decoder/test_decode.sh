#!/bin/bash

../maindaq-decode --verbose --file run_009760.dat --server seaquel.physics.illinois.edu --schema user_test_9760 --sampling 100 --offline --dir /var/tmp --root ../user_test_9760.root --verbose
#../maindaq-decode --verbose --file /pnfs/e906/data/mainDAQ/run_012606.dat --server e906-db3.fnal.gov --schema run_012606_R000 --sampling 1 --offline --dir /var/tmp --root ../user_test_12606.root
