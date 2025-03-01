from qgis.core import *
from qgis.utils import *
from PyQt5.QtWidgets import *

import os
import time
import pandas as pd
import psycopg2
from psycopg2 import sql
import sys
import os.path
from datetime import datetime
import processing
import traceback

class RUN_BUTTON:

    def __init__(self, iface, dlg,states):
        self.iface = iface 
        self.Wireless_dialog = dlg
        self.dlg = dlg
        self.db_connection = None

        self.host = "localhost"
        self.dbname = "wireless"
        self.user = "postgres"
        self.password = None
        self.port = "5432"
        self.conn = None
        self.cur = None

        self.USstates = states
    
    def connect(self):
        """Method to connect to the database."""
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.cur = self.conn.cursor()
            # print("Connected to the database")
        except Exception as e:
            print("Error connecting to the database:", e)

    def execute_query(self, query, params=None):
        """Method to execute a query."""
        if not self.cur:
            print("Not connected to the database.")
            return None
        
        try:
            self.cur.execute(query, params)
            return self.cur.fetchall()  # Returns query results
        except Exception as e:
            print("Error executing query:", e)
            return None
    
    def close(self):
        """Method to close the database connection."""
        if self.cur:
            self.cur.close()
            # print("Cursor closed")
        if self.conn:
            self.conn.close()
            # print("Connection closed")
   
    def output_check(self):
        """Checks if an output path has been selected."""
        # Get file path from the file widget
        output_dir = self.dlg.output_dir.filePath()

        # Execute the function, passing output_dir
        self.running_function(output_dir, self.Wireless_dialog, self.iface,
                              self.USstates[self.Wireless_dialog.StatescomboBox.currentIndex()])

    def show_warning(self, message):
        """Show a warning message on the alert window"""
        self.Wireless_dialog.hide()
        self.message = QMessageBox()
        self.message.setWindowTitle("Warning")
        self.message.setIcon(QMessageBox.Warning)
        self.message.setText(message)
        self.message.exec()
        self.Wireless_dialog.show()

    def get_password(self):
            """Open a window to insert the password."""
            mypassword, ok = QInputDialog.getText(None, 'Password Required', 'Please enter your password:', QLineEdit.Password)
            return mypassword if ok else None


    def running_function(self, output_dir, Wireless_dialog, iface, state):
        """This is the function where the real work happens. Here are called all the 
        SQL functions (function 1 step to 7.1. step) and the function to intersect
        and export the output data into a user-made folder"""

        Wireless_dialog.close()

        # check the output_dir
        if not output_dir:
            self.show_warning("No output path has been chosen.")
            return
        
        # open py console
        pythonConsole = iface.mainWindow().findChild(QDockWidget, 'PythonConsole')
        if not pythonConsole or not pythonConsole.isVisible():
            iface.actionShowPythonDialog().trigger()
        
        # preliminary work to set the proper US State
        print ("Analyzing " + state)
        list = str(state).split()                # e.g. '01 - ALABAMA'
        state_numbers = list[0]                  # aka '01'
   
        """ Here it set the input parameters """
        #input layers
        potential_clients = Wireless_dialog.PCComboBox.currentLayer()
        areas_interest = Wireless_dialog.AIComboBox.currentLayer()
        towers = Wireless_dialog.TComboBox.currentLayer()
        fiber_points = Wireless_dialog.FComboBox.currentLayer()
        
        #output path
        output_dir = self.Wireless_dialog.output_dir.filePath()


        # database connection
        mypassword = self.get_password()
        self.password = mypassword
        self.connect()

        start_time = datetime.now()

        """ Here it process the real work functions """
        print(potential_clients,areas_interest,towers,fiber_points)

        # 1. Intersect the potential clients with the areas of interest
        intersected_PC_AI = self.intersect_PC_AI(potential_clients,areas_interest)
        if not intersected_PC_AI:
            print ("error in intersect_PC_AI")
            return None
        # 2.Calculate the weighted centroid of the potential clients in each area of interest
        


    #Intersect the potential clients with the areas of interest
    def intersect_PC_AI(self,potential_clients,areas_interest):
        """Creates a unified table and updates the concatenated column"""
        inittime = datetime.now()

        # Set the variables
        PC_name = potential_clients.name()
        AI_name = areas_interest.name()
        schema_name = "example" 

        try:
                query = f"""
                    -- Verify if the table exist
                        DO $$ 
                        BEGIN
                            IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_name = '{PC_name}') THEN
                                RAISE EXCEPTION 'Table {PC_name} does not exist in schema {schema_name}. Aborting.';
                            END IF;
                            IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_name = '{AI_name}') THEN
                                RAISE EXCEPTION 'Table {AI_name} does not exist in schema {schema_name}. Aborting.';
                            END IF;

                            DROP TABLE IF EXISTS {schema_name}.intersect_AI_PC;

                            CREATE TABLE {schema_name}.intersect_AI_PC AS
                            SELECT e.*, p.id AS polygon_id
                            FROM example.{PC_name} e
                            JOIN example.{AI_name} p
                            ON ST_Contains(p.geom, e.geom);
                        END $$;
                    """
                # print(f"Executing query: {query}")
                self.cur.execute(query)
                
                # Commit the modification in the database
                self.conn.commit()

                # Verify if teh table was created
                self.cur.execute(f"SELECT to_regclass('intersect_AI_PC');")
                result = self.cur.fetchone()
                if result and result[0]:
                    print(f"Table intersect_AI_PC exists.")
                else:
                    print(f"Table intersect_AI_PC was NOT created.")

        except Exception as e:
                print(f"Error: {e}")
                self.conn.rollback()  # In case of error, follow the rollback

                if self.cur:
                    self.cur.close()
                if self.conn:
                    self.conn.close() 

        print('0.1 . Runtime: creating a merged FCC_table data ' + str((datetime.now() - inittime).total_seconds()))
        # print("... \n")
        return True

    #Calculate the weighted centroid of the potential clients in each area of interest
    def weighted_centroids(self,potential_clients,areas_interest):
        inittime = datetime.now()

        # Set the variables
        PC_name = potential_clients.name()
        AI_name = areas_interest.name()
        schema_name = "example"

        try:
                query=f"""
                    -- Calculate the average centroids of the potetial clients
                        WITH weighted_centroids AS (
                        SELECT 
                            p.fid AS polygon_id,
                            AVG(ST_X(e.geom)) AS avg_x, -- Average of x cordinates 
                            AVG(ST_Y(e.geom)) AS avg_y  -- Average of y cordinates 
                        FROM FROM example.{PC_name} e
                        JOIN example.{AI_name} p
                        ON ST_Contains(p.geom, e.geom)
                        GROUP BY p.fid
                        )
                        
                        -- Update the areas of interest table with the centroid of potential clientss
                        UPDATE example.{AI_name} p
                        SET centroid_weighted = ST_SetSRID(ST_MakePoint(wc.avg_x, wc.avg_y), 4326)
                        FROM weighted_centroids wc
                        WHERE spa.fid = wc.polygon_id;
                        """
        except Exception as e:
                print(f"Error: {e}")
                self.conn.rollback()  # In case of error, follow the rollback

                if self.cur:
                    self.cur.close()
                if self.conn:
                    self.conn.close() 

        print('0.1 . Runtime: creating a merged FCC_table data ' + str((datetime.now() - inittime).total_seconds()))
        # print("... \n")
        return True
            