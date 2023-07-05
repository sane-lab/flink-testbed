#!/bin/bash
kill -9 $(jps | grep ConsoleConsumer | awk '{print $1}')
