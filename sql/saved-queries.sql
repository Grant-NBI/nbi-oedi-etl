/*
 Run your regular or recurring queries here and save them for future use
 
 Placeholder values will be replaced by deployment environment  infrastructure resources during deployment
 
 !Note: using building type and county pertinent for AK since the test is built using state=AK (the smallest population dataset, and the CA takes too long). Edit this as necessary for your dataset.
 
 !This is not intended to run your adhoc queries
 */
-- label: Total Number of Individual Building Models
-- description: Finds the total number of unique hospital building models in Ketchikan Gateway Borough, AK.
SELECT COUNT(DISTINCT bldg_id) AS total_buildings
FROM $ { glue_db }.$ { glue_table }
WHERE in.state = 'AK'
    AND in.county_name = 'AK, Ketchikan Gateway Borough'
    AND in.comstock_building_type = 'Hospital';
-- label: Number of Buildings by Building Type Group
-- description: Segregates the building models by their type group and counts the number of unique buildings for each type group in Ketchikan Gateway Borough, AK.
SELECT in.comstock_building_type_group,
    COUNT(DISTINCT bldg_id) AS num_buildings
FROM $ { glue_db }.$ { glue_table }
WHERE in.state = 'AK'
    AND in.county_name = 'AK, Ketchikan Gateway Borough'
GROUP BY in.comstock_building_type_group;
-- label: Isolated Individual Building Models
-- description: Retrieves up to 500 individual hospital building models for the Healthcare building type group, based on the input criteria in Ketchikan Gateway Borough, AK.
WITH filtered_buildings AS (
    SELECT bldg_id,
        in.comstock_building_type_group,
        ROW_NUMBER() OVER (
            PARTITION BY in.comstock_building_type_group
            ORDER BY bldg_id
        ) AS rn
    FROM $ { glue_db }.$ { glue_table }
    WHERE in.state = 'AK'
        AND in.county_name = 'AK, Ketchikan Gateway Borough'
)
SELECT bldg_id,
    in.comstock_building_type_group
FROM filtered_buildings
WHERE (
        in.comstock_building_type_group = 'Healthcare'
        AND rn <= 500
    );