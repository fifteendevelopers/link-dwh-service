CREATE TABLE Sync_Log (
                          Table_Name VARCHAR(50) PRIMARY KEY,
                          Last_Synced_At DATETIME,
                          Records_Processed INT
);

-- =============================================
-- 1. REFERENCE DIMENSIONS
-- =============================================

CREATE TABLE Dim_Source_System (
                                   Source_System_Key INT AUTO_INCREMENT PRIMARY KEY,
                                   System_Name       VARCHAR(50),
                                   System_Type       VARCHAR(50),
                                   Last_Sync_Date    DATETIME
);

CREATE TABLE Dim_Date (
                          Date_Key               INT PRIMARY KEY, -- Format: 20260225
                          Full_Date              DATE NOT NULL,
                          Month_Name             VARCHAR(10),
                          Calendar_Year          INT,
                          Financial_Year         VARCHAR(10),     -- e.g., "FY 2025-26"
                          Financial_Quarter      CHAR(2),         -- Q1, Q2, Q3, Q4
                          Financial_Month_Number INT,             -- April = 1
                          Is_Weekend             BOOLEAN,
                          UNIQUE INDEX idx_full_date (Full_Date)
);

CREATE TABLE Dim_Send_Code (
                               Send_Code_Key   INT AUTO_INCREMENT PRIMARY KEY,
                               Send_Code       VARCHAR(20) UNIQUE,
                               Description     VARCHAR(255) NULL -- Can be manually updated later if needed
);

-- =============================================
-- 2. MASTER DIMENSIONS (Shared across Galaxy)
-- =============================================

CREATE TABLE Dim_Grant_Recipient (
                                     Recipient_Key        INT AUTO_INCREMENT PRIMARY KEY,
                                     Source_Recipient_Id  BIGINT UNSIGNED,
                                     Source_System_Key    INT,
                                     Recipient_Name       VARCHAR(255),
                                     Recipient_Number     VARCHAR(255),
                                     LA_Id                VARCHAR(255),
                                     Is_Active            CHAR(1), -- 'Y' or 'N' (Source Status)
    -- SCD Type 2 Columns
                                     Valid_From_Date      DATE NOT NULL,
                                     Valid_To_Date        DATE NOT NULL DEFAULT '9999-12-31',
                                     Is_Current           BOOLEAN DEFAULT 1,

                                     FOREIGN KEY (Source_System_Key) REFERENCES Dim_Source_System(Source_System_Key),
                                     INDEX idx_gr_source (Source_Recipient_Id, Is_Current)
);

CREATE TABLE Dim_Grant (
                           Grant_Key         INT AUTO_INCREMENT PRIMARY KEY,
                           Source_Grant_Id   BIGINT UNSIGNED,
                           Source_System_Key INT,
                           Grant_Recipient_Key  INT,
                           Grant_Number        VARCHAR(255),
                           Grant_Label        VARCHAR(255),
                           Grant_Period_Start_Year      SMALLINT,
                           Grant_Source        VARCHAR(255),
                           FOREIGN KEY (Grant_Recipient_Key) REFERENCES Dim_Grant_Recipient(Recipient_Key),
                           FOREIGN KEY (Source_System_Key) REFERENCES Dim_Source_System(Source_System_Key)
);

CREATE TABLE Dim_School (
                            School_Key        INT AUTO_INCREMENT PRIMARY KEY,
                            Source_School_Id  INT,
                            Source_System_Key INT,
                            School_Urn        VARCHAR(255),
                            School_Name       VARCHAR(255),
                            LA_Code           VARCHAR(255),
                            LA_Name           VARCHAR(255),
                            FOREIGN KEY (Source_System_Key) REFERENCES Dim_Source_System(Source_System_Key)
);

CREATE TABLE Dim_Organisation (
                                  Organisation_Key      INT AUTO_INCREMENT PRIMARY KEY,
                                  Source_Organisation_Id BIGINT UNSIGNED,
                                  Source_System_Key     INT,
                                  Provider_Key          INT,
                                  Organisation_Name     VARCHAR(255),
                                  FOREIGN KEY (Source_System_Key) REFERENCES Dim_Source_System(Source_System_Key),
                                  FOREIGN KEY (Provider_Key) REFERENCES Dim_Training_Provider(Provider_Key)
);

CREATE TABLE Dim_Training_Provider (
                                       Provider_Key         INT AUTO_INCREMENT PRIMARY KEY,
                                       Source_Provider_Id   BIGINT UNSIGNED,
                                       Source_System_Key    INT,
                                       Provider_Name        VARCHAR(255),
                                       Provider_Number      VARCHAR(255),
                                       Is_Active            CHAR(1), -- 'Y' or 'N' (Source Status)
    -- SCD Type 2 Columns
                                       Valid_From_Date      DATE NOT NULL,
                                       Valid_To_Date        DATE NOT NULL DEFAULT '9999-12-31',
                                       Is_Current           BOOLEAN DEFAULT 1,

                                       FOREIGN KEY (Source_System_Key) REFERENCES Dim_Source_System(Source_System_Key),
                                       INDEX idx_tp_source (Source_Provider_Id, Is_Current)
);

CREATE TABLE Dim_Delivery_Header (
                                     Delivery_Key       INT AUTO_INCREMENT PRIMARY KEY,
                                     Source_Delivery_Id BIGINT UNSIGNED,
                                     Source_System_Key  INT,
                                     Grant_Key          INT,
                                     School_Key         INT,
                                     Organisation_Key   INT,
                                     Training_Provider_Key INT,
                                     Delivery_Status    VARCHAR(50),
                                     Date_Delivery_Start     DATE,
                                     Date_Delivery_End     DATE,
                                     Digitisation_Booking   TINYINT(1),
                                     FOREIGN KEY (Source_System_Key) REFERENCES Dim_Source_System(Source_System_Key),
                                     FOREIGN KEY (Grant_Key) REFERENCES Dim_Grant(Grant_Key),
                                     FOREIGN KEY (School_Key) REFERENCES Dim_School(School_Key),
                                     FOREIGN KEY (Organisation_Key) REFERENCES Dim_Organisation(Organisation_Key),
                                     FOREIGN KEY (Training_Provider_Key) REFERENCES Dim_Training_Provider(Provider_Key)
);

CREATE TABLE Dim_Course (
                            Course_Key        INT AUTO_INCREMENT PRIMARY KEY,
                            Source_Course_Id  BIGINT UNSIGNED,
                            Source_System_Key INT,
                            Delivery_Key INT,
                            Course_Level      VARCHAR(45),
                            Status INT, -- TODO: possible transform status INT to String value
                            Start_Date DATE,
                            Date_Complete DATE,
                            Year_Group VARCHAR(45),
                            Parent_Course_Key INT,
                            FOREIGN KEY (Delivery_Key) REFERENCES Dim_Delivery_Header(Delivery_Key),
                            FOREIGN KEY (Source_System_Key) REFERENCES Dim_Source_System(Source_System_Key),
                            FOREIGN KEY (Parent_Course_Key) REFERENCES Dim_Course(Course_Key)
);

CREATE TABLE Dim_Rider (
                           Rider_Key         INT AUTO_INCREMENT PRIMARY KEY,
                           Source_Rider_Id   BIGINT UNSIGNED,
                           Source_System_Key INT,
                           School_Key        INT,
                           Ethnicity         VARCHAR(50),
                           Gender            VARCHAR(20),
                           Pupil_Premium     TINYINT(1),
                           Has_Send          TINYINT(1),
                           FOREIGN KEY (Source_System_Key) REFERENCES Dim_Source_System(Source_System_Key),
                           FOREIGN KEY (School_Key) REFERENCES Dim_School(School_Key)
);

CREATE TABLE Dim_Consent (
                             Consent_Key            INT AUTO_INCREMENT PRIMARY KEY,
                             Source_Consent_Id      BIGINT UNSIGNED,
                             Source_System_Key      INT,
                             Rider_Key              INT,
                             Delivery_Key           INT,
                             Consent_Status         TINYINT,
                             Has_Bike               TINYINT,
                             Cycle_Ability_Raw      TEXT, -- JSON from cycle_ability
                             Is_FSM                 TINYINT,
                             Is_SEND                TINYINT,
                             Has_Medical_Condition  TINYINT,
                             Attended               TINYINT,
                             Gender                 VARCHAR(50),
                             Ethnicity              VARCHAR(100),

                             FOREIGN KEY (Source_System_Key) REFERENCES Dim_Source_System(Source_System_Key),
                             FOREIGN KEY (Rider_Key) REFERENCES Dim_Rider(Rider_Key),
                             FOREIGN KEY (Delivery_Key) REFERENCES Dim_Delivery_Header(Delivery_Key),
                             INDEX idx_consent_source (Source_Consent_Id)
);

-- a mapping table between Rider and Send Codes
CREATE TABLE Map_Rider_Send (
                                Rider_Key       INT,
                                Send_Code_Key   INT,
                                PRIMARY KEY (Rider_Key, Send_Code_Key),
                                FOREIGN KEY (Rider_Key) REFERENCES Dim_Rider(Rider_Key),
                                FOREIGN KEY (Send_Code_Key) REFERENCES Dim_Send_Code(Send_Code_Key)
);


-- =============================================
-- 3. FACT TABLES (The Galaxy Hubs)
-- =============================================

-- Fact A: Performance & Demographics
CREATE TABLE Fact_Course_Delivery (
                                      Delivery_Fact_Key INT AUTO_INCREMENT PRIMARY KEY,
                                      Date_Key          INT,
                                      Delivery_Key      INT,
                                      School_Key        INT,
                                      Course_Key        INT,
                                      Provider_Key      INT,
                                      Grant_Key         INT,

    -- Activity Metrics
                                      Riders_Enrolled_Count  INT,
                                      Riders_Completed_Count INT,
                                      Instructor_Count       INT,
                                      Total_Cost             DECIMAL(10, 2),

    -- Demographic Metrics (Aggregated from TP Exports)
                                      Count_Female           INT DEFAULT 0,
                                      Count_Male             INT DEFAULT 0,
                                      Count_Ethnicity_White  INT DEFAULT 0,
                                      Count_Ethnicity_Asian  INT DEFAULT 0,
                                      Count_Ethnicity_Black  INT DEFAULT 0,
                                      Count_Ethnicity_Mixed  INT DEFAULT 0,
                                      Count_Ethnicity_Other  INT DEFAULT 0,

                                      FOREIGN KEY (Date_Key) REFERENCES Dim_Date(Date_Key),
                                      FOREIGN KEY (Delivery_Key) REFERENCES Dim_Delivery_Header(Delivery_Key),
                                      FOREIGN KEY (School_Key) REFERENCES Dim_School(School_Key),
                                      FOREIGN KEY (Course_Key) REFERENCES Dim_Course(Course_Key),
                                      FOREIGN KEY (Provider_Key) REFERENCES Dim_Training_Provider(Provider_Key),
                                      FOREIGN KEY (Grant_Key) REFERENCES Dim_Grant(Grant_Key)
);

-- Fact B: Hands Up Survey (Aggregated Tally)
CREATE TABLE Fact_HandsUp_Survey (
                                     Survey_Fact_Key        INT AUTO_INCREMENT PRIMARY KEY,
                                     Date_Key               INT,
                                     Delivery_Key           INT,
                                     School_Key             INT,
                                     Course_Key             INT,

    -- Question Tallies
                                     Total_Pupils_Surveyed  INT,
                                     Enjoyed_Count          INT DEFAULT 0,
                                     Feel_More_Safe_Count   INT DEFAULT 0,
                                     Feel_More_Confident_Count INT DEFAULT 0,
                                     Cycle_To_School_Before INT DEFAULT 0,
                                     Cycle_To_School_After  INT DEFAULT 0,

                                     FOREIGN KEY (Date_Key) REFERENCES Dim_Date(Date_Key),
                                     FOREIGN KEY (Delivery_Key) REFERENCES Dim_Delivery_Header(Delivery_Key),
                                     FOREIGN KEY (School_Key) REFERENCES Dim_School(School_Key),
                                     FOREIGN KEY (Course_Key) REFERENCES Dim_Course(Course_Key)
);

-- Fact C: Parent Post-Course Survey (Individual Impact)
CREATE TABLE Fact_Parent_Survey (
                                    Parent_Survey_Key        INT AUTO_INCREMENT PRIMARY KEY,
                                    Date_Key                 INT,
                                    Rider_Key                INT,
                                    Course_Key               INT,
                                    Grant_Key                INT,

    -- Individual Scores
                                    Riding_Frequency_Score   INT,
                                    Pre_Confidence_Score     INT,
                                    Post_Confidence_Score    INT,
                                    Skill_Improvement_Rating INT,

                                    FOREIGN KEY (Date_Key) REFERENCES Dim_Date(Date_Key),
                                    FOREIGN KEY (Rider_Key) REFERENCES Dim_Rider(Rider_Key),
                                    FOREIGN KEY (Course_Key) REFERENCES Dim_Course(Course_Key),
                                    FOREIGN KEY (Grant_Key) REFERENCES Dim_Grant(Grant_Key)
);

-- Seeding Data

-- Source System;
INSERT INTO Dim_Source_System (System_Name,System_Type)
VALUES ('link','laravel');

SET SESSION cte_max_recursion_depth = 50000;
SET @start_date = '2019-01-01';
SET @end_date   = '2050-12-31';

-- Loop to populate dates
INSERT INTO Dim_Date (
    Date_Key,
    Full_Date,
    Month_Name,
    Calendar_Year,
    Financial_Year,
    Financial_Quarter,
    Financial_Month_Number,
    Is_Weekend
)
WITH RECURSIVE DateRange AS (
    SELECT @start_date AS d
    UNION ALL
    SELECT d + INTERVAL 1 DAY FROM DateRange WHERE d < @end_date
    )
SELECT
    REPLACE(d, '-', '') AS Date_Key,
    d AS Full_Date,
    MONTHNAME(d) AS Month_Name,
    YEAR(d) AS Calendar_Year,
    -- Financial Year Logic (Starts April 1st)
    CASE
    WHEN MONTH(d) >= 4 THEN CONCAT('FY ', YEAR(d), '-', SUBSTRING(YEAR(d) + 1, 3, 2))
    ELSE CONCAT('FY ', YEAR(d) - 1, '-', SUBSTRING(YEAR(d), 3, 2))
END AS Financial_Year,
    -- Financial Quarter Logic
    CASE
        WHEN MONTH(d) IN (4, 5, 6) THEN 'Q1'
        WHEN MONTH(d) IN (7, 8, 9) THEN 'Q2'
        WHEN MONTH(d) IN (10, 11, 12) THEN 'Q3'
        ELSE 'Q4'
END AS Financial_Quarter,
    -- Financial Month (April = 1)
    CASE
        WHEN MONTH(d) >= 4 THEN MONTH(d) - 3
        ELSE MONTH(d) + 9
END AS Financial_Month_Number,
    -- Weekend Flag
    CASE WHEN DAYOFWEEK(d) IN (1, 7) THEN TRUE ELSE FALSE END AS Is_Weekend
FROM DateRange;
