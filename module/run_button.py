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
      
        # Dictionary mapping FIPS codes to UTM EPSG codes
        fips_to_epsg = {
            1: 32616,  2: 32604,  4: 32612,  5: 32615,  6: 32611,  8: 32613,  9: 32618, 10: 32618,
            12: 32617, 13: 32617, 15: 32604, 16: 32612, 17: 32616, 18: 32616, 19: 32615, 20: 32614,
            21: 32616, 22: 32615, 23: 32619, 24: 32618, 25: 32619, 26: 32616, 27: 32615, 28: 32616,
            29: 32615, 30: 32613, 31: 32614, 32: 32611, 33: 32619, 34: 32618, 35: 32613, 36: 32618,
            37: 32617, 38: 32614, 39: 32617, 40: 32614, 41: 32611, 42: 32618, 44: 32619, 45: 32617,
            46: 32614, 47: 32616, 48: 32614, 49: 32612, 50: 32618, 51: 32618, 53: 32610, 54: 32617,
            55: 32616, 56: 32613
        }

        state_epsg = fips_to_epsg.get(int(state_numbers))

        """ Here it set the input parameters """
        #input layers
        potential_clients = Wireless_dialog.PCComboBox.currentLayer()
        areas_interest = Wireless_dialog.AIComboBox.currentLayer()
        towers = Wireless_dialog.TComboBox.currentLayer()
        fiber_points = Wireless_dialog.FComboBox.currentLayer()
        PC_buffer_miles = Wireless_dialog.Cbuffer.text()
        T_buffer_miles = Wireless_dialog.Tbuffer.text()
        #output path
        output_dir = self.Wireless_dialog.output_dir.filePath()


        # database connection
        mypassword = self.get_password()
        self.password = mypassword
        self.connect()

        """ Here it process the real work functions """
        # 1. Intersect the potential clients with the areas of interest
        intersected_PC_AI = self.intersect_PC_AI(potential_clients,areas_interest)
        if not intersected_PC_AI:
            print ("error in intersect_PC_AI")
            return None
        # 2.Calculate the weighted centroid of the potential clients in each area of interest
        weighted_centroids_calculated = self.weighted_centroids(potential_clients,areas_interest)
        if not weighted_centroids_calculated:
            print ("error in weighted_centroids")
            return None
        # 3.Create the buffer around the centroid of the potential clients
        bufferPC_calculated = self.bufferPC(PC_buffer_miles,state_epsg,areas_interest)
        if not bufferPC_calculated:
            print ("error in buffer_PC_centroids")
            return None
        # 4.Create the buffer around the towers
        bufferT_calculated = self.bufferT(T_buffer_miles,state_epsg,towers)
        if not bufferT_calculated:
            print ("error in bufferT")
            return None

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
                            FROM {schema_name}.{PC_name} e
                            JOIN {schema_name}.{AI_name} p
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

        print('1 . Runtime: creating a intersect_PC_AI ' + str((datetime.now() - inittime).total_seconds()))
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
                   -- Check the column exists
                    DO $$ 
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 
                            FROM information_schema.columns 
                            WHERE table_schema = '{schema_name}' 
                            AND table_name = '{AI_name}' 
                            AND column_name = 'centroid_weighted'
                        ) THEN
                            ALTER TABLE {schema_name}.{AI_name} 
                            ADD COLUMN centroid_weighted geometry(Point, 4326);
                        END IF;
                    END $$;

                    -- Create the CTE and update
                    WITH weighted_centroids AS (
                        SELECT 
                            p.id AS polygon_id,
                            AVG(ST_X(e.geom)) AS avg_x, -- average of X coordinates 
                            AVG(ST_Y(e.geom)) AS avg_y  -- average of Y coordinates 
                        FROM {schema_name}.{PC_name} e
                        JOIN {schema_name}.{AI_name} p
                        ON ST_Contains(p.geom, e.geom)
                        GROUP BY p.id
                    )
                    UPDATE {schema_name}.{AI_name} p
                    SET centroid_weighted = ST_SetSRID(ST_MakePoint(wc.avg_x, wc.avg_y), 4326)
                    FROM weighted_centroids wc
                    WHERE p.id = wc.polygon_id;
                        """
                # print(f"Executing query: {query}")
                self.cur.execute(query)
                
                # Commit the modification in the database
                self.conn.commit()

        except Exception as e:
                print(f"Error: {e}")
                self.conn.rollback()  # In case of error, follow the rollback

                if self.cur:
                    self.cur.close()
                if self.conn:
                    self.conn.close() 

        print('2 . Runtime: creating a weighted_centroids ' + str((datetime.now() - inittime).total_seconds()))
        return True
            
    #Create the buffer around the centroid of the potential clients
    def bufferPC(self, PC_buffer_miles,state_epsg,areas_interest ):
        inittime = datetime.now()

        # Set the variables
        schema_name = "example"
        buffer_meters = float(PC_buffer_miles) * 1609.34
        print(buffer_meters)
        AI_name = areas_interest.name()

        try:
            query=f"""
                --> 
                    -- Drop the table if it exists
                    DROP TABLE IF EXISTS {schema_name}.buffer_{PC_buffer_miles}_miles;

                    -- Create new table with all columns and transformed geometry
                    CREATE TABLE {schema_name}.buffer_{PC_buffer_miles}_miles AS
                    SELECT 
                        id, 
                        ST_Transform(centroid_weighted, {state_epsg}) AS geom_utm
                    FROM {schema_name}.{AI_name};
 
                  -- Create a new column with the geometry
                    ALTER TABLE {schema_name}.buffer_{PC_buffer_miles}_miles 
                    ADD COLUMN buffer_{PC_buffer_miles}_miles geometry(Polygon, {state_epsg});

                    -- Calculate the buffer in meters around the centroids
                    UPDATE {schema_name}.buffer_{PC_buffer_miles}_miles 
                    SET buffer_{PC_buffer_miles}_miles = ST_Buffer(geom_utm, {buffer_meters});
                """
                
            self.cur.execute(query)
                
            # Commit the modification in the database
            self.conn.commit()
            
        except Exception as e:
                print(f"Error: {e}")
                self.conn.rollback()  # In case of error, follow the rollback

                if self.cur:
                    self.cur.close()
                if self.conn:
                    self.conn.close()
        
        print('3 . Runtime: creating a buffer around potential clients ' + str((datetime.now() - inittime).total_seconds()))
        return True

    #Create the buffer around the towers
    def bufferT(self, T_buffer_miles,state_epsg,towers):
        inittime = datetime.now()

        # Set the variables
        schema_name = "example"
        buffer_meters = float(T_buffer_miles) * 1609.34
        print(buffer_meters)
        T_name = towers.name()

        try:
            query=f"""
                --> 
                    -- Drop the table if it exists
                    DROP TABLE IF EXISTS {schema_name}.buffer_tower_{T_buffer_miles}_miles;

                    -- Create new table with all columns and transformed geometry
                    CREATE TABLE {schema_name}.buffer_tower_{T_buffer_miles}_miles AS
                    SELECT 
                        id, 
                        ST_Transform(geom, {state_epsg}) AS geom_utm
                    FROM {schema_name}.{T_name};
 
                  -- Create a new column with the geometry
                    ALTER TABLE {schema_name}.buffer_tower_{T_buffer_miles}_miles 
                    ADD COLUMN buffer_tower_{T_buffer_miles}_miles geometry(Polygon, {state_epsg});

                    -- Calculate the buffer in meters around the centroids
                    UPDATE {schema_name}.buffer_tower_{T_buffer_miles}_miles 
                    SET buffer_tower_{T_buffer_miles}_miles = ST_Buffer(geom_utm, {buffer_meters});
                """
                
            self.cur.execute(query)
                
            # Commit the modification in the database
            self.conn.commit()
            
        except Exception as e:
                print(f"Error: {e}")
                self.conn.rollback()  # In case of error, follow the rollback

                if self.cur:
                    self.cur.close()
                if self.conn:
                    self.conn.close()
        
        print('4 . Runtime: creating a buffer around towers' + str((datetime.now() - inittime).total_seconds()))
        return True
