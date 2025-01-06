import uuid
import boto3
from fastapi import FastAPI, BackgroundTasks, HTTPException
from pydantic import BaseModel
from fastapi import FastAPI, File, UploadFile, BackgroundTasks, HTTPException
import requests
from typing import Dict
from typing import Union
import pandas as pd
from fastapi import FastAPI,BackgroundTasks,HTTPException, UploadFile
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
import re
import ftfy
from tqdm import tqdm
from io import BytesIO
import io
import boto3
import os
from botocore.exceptions import NoCredentialsError
from grpc import Status
from dotenv import load_dotenv

app = FastAPI()

load_dotenv()

s3bucket=os.getenv("aws_s3_bucket")
AWS_S3_BUCKET =s3bucket
aws_access_key_id= os.getenv("aws_access_key_id")
aws_secret_access_key = os.getenv("aws_secret_access_key")
s3_client = boto3.client("s3",aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key)
BUCKET_NAME =s3bucket


processed_files = {}
class ProcessSheetRequest(BaseModel):
    sheet_url: str


'''function where uploading the excel file takes place'''
def upload_to_s3(file_data: bytes, s3_filename: str) -> str:
    try:
        Key='images/Temp_BAU_outfile/amzonfilestitle/{}'.format(s3_filename)
        s3_client.upload_fileobj(BytesIO(file_data), BUCKET_NAME, Key)
        s3_url = f"https://{BUCKET_NAME}.s3.ap-south-1.amazonaws.com/{Key}"
        return s3_url
    except (NoCredentialsError, PartialCredentialsError) as e:
        raise HTTPException(status_code=500, detail="Error in uploading file to S3")

'''calling the function where s3 link genration happens'''
def process_and_upload(file_data: bytes, s3_filename: str) -> None:
    upload_to_s3(file_data, s3_filename)

'''background task takes places while showing s3 file as output'''
@app.post("/upload/")
async def upload_file(background_tasks: BackgroundTasks, file: UploadFile = File(...)) -> Dict[str, str]: # Generate a unique task ID
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="Invalid file type. Only Excel files are allowed.")
    file_data = await file.read()
    s3_filename = f"{file.filename}"
    Key='images/Temp_BAU_outfile/amzonfilestitle/{}'.format(s3_filename)
    background_tasks.add_task(process_and_upload, file_data, s3_filename)
    s3_url = f"https://{BUCKET_NAME}.s3.ap-south-1.amazonaws.com/{Key}"
    return {"message": "File is being uploaded in the background.", "s3_url": s3_url}

def process_sheet(sheet_url: str, uuid_str: str,key1:Union[str, None] = None,value1:Union[str, None] = None,key2:Union[str, None] = None,value2:Union[str, None] = None,key3:Union[str, None] = None,value3:Union[str, None] = None):
    """Process the sheet and upload to S3."""
    newname= sheet_url.split('.xlsx')[0].split('/')[-1].replace('%20',' ') +' - output'
    # print(newname+' - output')
    newdictonary = dictonary(key1,key2,key3,value1,value2,value3)
    input_sheet_done = input_data(sheet_url)
    processed_sheet_done = processing_data(input_sheet_done,sheet_url,newname,newdictonary)
    keysremaning = processed_sheet_done[0]
    s3_link = processed_sheet_done[1]
    processed_files[uuid_str] = s3_link,keysremaning

    print(f"File processed and uploaded to S3: {s3_link}")
    return keysremaning, s3_link

@app.post("/process-sheet/")
async def process_sheet_endpoint(request: ProcessSheetRequest, background_tasks: BackgroundTasks,key1:Union[str, None] = None,value1:Union[str, None] = None,key2:Union[str, None] = None,value2:Union[str, None] = None,key3:Union[str, None] = None,value3:Union[str, None] = None):
    """Endpoint to trigger background task for sheet processing."""
    # Generate a new UUID for this request
    uuid_str = str(uuid.uuid4())
    
    # Add background task for processing
    background_tasks.add_task(process_sheet, request.sheet_url, uuid_str,key1,value1,key2,value2,key3,value3)
    
    # Return the UUID immediately to the user
    return {"uuid": uuid_str, "message": "Sheet processing started in the background"}

@app.get("/get-s3-link/{uuid_str}")
async def get_s3_link(uuid_str: str):
    """Endpoint to get the S3 link by UUID."""
    # Check if the UUID exists in the processed_files dictionary
    if uuid_str not in processed_files:
        raise HTTPException(status_code=404, detail="UUID not found or processing not completed")
    
    # Return the S3 link for the given UUID
    return {"uuid": uuid_str, "s3_link": processed_files[uuid_str][0],"remaningthing": processed_files[uuid_str][1]}


def dictonary(key1,key2,key3,value1,value2,value3):
        keyslistofnotmapped=[]
        valueslistofnotmapped=[]
        keyslistofnotmapped.append(key1)
        keyslistofnotmapped.append(key2)
        keyslistofnotmapped.append(key3)
        valueslistofnotmapped.append(value1)
        valueslistofnotmapped.append(value2)
        valueslistofnotmapped.append(value3)
        nedic={}
        correctvalueslistofnotmapped=[]
        correctkeyslistofnotmapped=[]
        for i in keyslistofnotmapped:
            if i != None:
                correctkeyslistofnotmapped.append(i)
        for i in valueslistofnotmapped:
            if i != None:
                correctvalueslistofnotmapped.append(i)
        for i in range(len(correctkeyslistofnotmapped)):
            nedic[correctkeyslistofnotmapped[i]]=correctvalueslistofnotmapped[i]
        return nedic

def processing_data(input_sheet_done,s3link2,excelsheetname,newdictonary):
    data=input_sheet_done
    attributeData = pd.read_excel(s3link2,'data sheet').to_dict('records')
    attributeMap = {
        "neck_style": "neck",
        "target_species": "target_species",
        "product_benefit": "product_benefit",
        "specialty": "specialty",
        "operation_mode": "operation_mode",
        "target_audience_keyword": "target_audience_keyword",
        "container_type": "container",
        "variety": "variety",
        "flavor_name": "flavor",
        "item_weight": "item_weight",
        "snack_chip_type": "snack_chip_type",
        "temperature_condition": "temperature_condition",
        "theme": "theme",
        "age_range_description": "age_range_description",
        "hardware_platform": "hardware_platform",
        "battery_cell_composition": "battery",
        "auto_part_position": "auto_part_position",
        "viscosity_unit": "viscosity",
        "compatible_with_vehicle_type": "compatible_with_vehicle_type",
        "graphics_ram_size": "graphics_ram",
        "graphics_ram_type": "graphics_ram",
        "efficiency": "efficiency",
        "cpu_speed_unit_of_measure": "computer_cpu_speed",
        "graphics_card_interface": "graphics_card_interface",
        "cpu_model_socket": "cpu_model",
        "motherboard_type": "motherboard_type",
        "memory_storage_capacity": "memory_storage_capacity",
        "hardware_interface": "hardware_interface",
        "connector_type": "connector_type",
        "display_technology": "display",
        "speaker_type": "speaker_type",
        "focal_length": "focal_length_description",
        "data_transfer_rate": "data_transfer_rate",
        "zoom_type": "zoom_type",
        "voltage_unit": "voltage",
        "wireless_communication_technology": "wireless_communication_technology",
        "digital_storage_capacity": "digital_storage_capacity",
        "ram_memory": "ram_memory",
        "ram_size": "ram_memory",
        "display_size_unit_of_measure": "display",
        "frequency_band_class": "frequency_band_class",
        "polar_pattern": "polar_pattern",
        "operating_system": "operating_system",
        "cpu_model_manufacturer": "cpu_model",
        "display_resolution_maximum": "display",
        "display_resolution_maximum_unit_of_measure": "display",
        "form_factor": "form_factor",
        "quantity": "item_package_quantity",
        "item_shape": "item_shape", 
        "material_type": "material",
        "color_name": "color",
        "bulb_base": "base_type",
        "count": "unit_count",
        "mounting_type": "mounting_type",
        "special_feature": "special_feature",
        "light_source_type": "light_source",
        "power_source_type": "power_source_type",
        "finish_type": "finish_type",
        "name_of_product": "ASIN", 
        "frame_type": "frame",
        "item_type_name": "item_type_name",
        "style_name": "style",
        "size_name": "size",
        "thread_count": "thread",
        "unit_of_measure": "measurement_system",
        "circuit_type": "ASIN",
        "included_components": "included_components",
        "department_name": "department",
        "occasion_type": "occasion_type",
        "compatible_devices": "compatible_devices",
        "connectivity_technology": "connectivity_technology",
        "unit_count_type": "ASIN",
        "heating_element_type": "heating_element_type",
        "item_diameter_unit_of_measure": "item_diameter",
        "purification_method": "purification_method",
        "capacity_unit_of_measure": "capacity",
        "wattage_unit_of_measure": "wattage",
        "blade_material_type": "blade",
    #     "energy_star": "energy_star",
        "installation_type": "installation_type",
        "filter_type": "filter_type",
        "form_factor": "form_factor",
        "scent_name": "deprecated_variation_theme",
        "item_form": "item_form",
        "fit_type": "fit_type",
        "sport_type": "sport_type",
        "top_style": "top_style",
        "wheel_type": "wheel",
        "pattern_name": "pattern",
        "maximum_weight_capacity_unit_of_measure": "maximum_weight_recommendation",
        "adjustment_type": "adjustment_type",
        "pattern_type": "pattern_type",
        "bike_type": "bike_type",
        "wheel_size": "wheel",
        "frame_size": "bullet_point",
        "hair_type": "hair_type",
        "item_length_unit_of_measure": "item_length",
        "polarization_type": "polarization_type",
        "item_len_des": "item_length_description",
        "length_longer_edge": "item_length",
        "surface_recommendation": "surface_recommendation",
        "length_longer_edge_unit_of_measure": "item_length",
        "display_type": "display",
        "width_shorter_edge": "item_length_width",
        "item_width_unit_of_measure": "item_length_width",
        "head_style": "head",
        "access_location": "access_location",
        "specific_uses_for_product": "specific_uses_for_product",
        "sleeve_type": "sleeve",
        "item_height": "item_length_width_height",
        "outer_material": "outer",
        "opening_mechanism": "opening_mechanism",
        "point_type": "point",
        "shank_type": "shank",
        "opacity": "opacity",
        "analog_format": "analog_video_format",
    #     "maximum_chuck_size": "",
        "subject_character": "subject_character",
        "item_length_description": "item_length_description",
        "bristle_type":"bristle",
        "shelf_type":"fc_shelf_life",
        "handle_material":"handle",
        "number_of_pages":"pages",
        "brightness":"brightness",
        "item_volume_unit_of_measure":"item_volume",
        "output_wattage":"output_wattage",
        "paint_type":"paint_type",
        "sheet_count":"sheet_count",
        "ink_color":"ink",
        "item_weight_unit_of_measure":"item_weight",
        "item_styling":"item_styling",
        "item_length":"item_length",
        "frame_material":"material",
        "fabric_type":"fabric_type",
        "item_capacity_unit_of_measure":"capacity",
        "maximum_operating_pressure_unit_of_measure":"maximum_operating_pressure",
        "paper_weight":"paper_weight",
        "maximum_weight_recommendation_unit_of_measure":"display_maximum_weight_recommendation",
        "brightness_unit_of_measure":"brightness",
        "light_type":"light_type",
        "fuel_type":"fuel_type",
        "item_dimension_unit_of_measure":"item_dimensions",
        "special_ingredient":"special_ingredient",
        "plant_or_animal_product_type":"plant_or_animal_product_type",
        "cable_length_unit_of_measure":"cable",
        "occupancy":"occupancy",
        "heater_surface_material":"heater_surface",
        "suspension_type":"suspension_type",
        "item_volume":"item_volume",
        "base_type":"base_type",
        "defrost_system_type":"defrost_system_type",
        "energy_star":"energy_star",
        "skin_type":"skin_type",
        "number_of_doors":"number_of_doors",
        "heating_method":"heating_method",
        "color_brightness":"color_brightness",
        "maximum_chuck_size":"maximum_chuck_size",
        "recommended_uses_for_product":"recommended_uses_for_product",
        "maximum_weight_recommendation":"maximum_weight_recommendation",
        "item_width":"item_width",
        "Item_shape":"item_shape",
        "lock_type":"lock_type",
        "number_of_keys":"number_of_keys",
        "paper_size":"paper_size",
        "unit_count":"unit_count",
        "Finish_Type":"finish_type",
        "band_material":"BandMaterial",
        "shell_type":"shell_type",
        "band_color":"band_color",
        "volume_capacity":"volume_capacity_name",
        "seating_capacity":"seating_capacity",
        "dial_color":"dial",
        "video_capture_resolution":"video_capture_resolution",
        "hardware_Interface":"hardware_Interface",
        "sun_protection_unit":"sun_protection",
        "plant_product_type":"plant_or_animal_product_type",
        "liquid_volume_unit_of_measure":"liquid_volume",
        "tea_variety":"tea_variety",
        "flavor":"flavor",
        "target_gender":"target_gender",
        "target_audience_keywords":"target_audience_keyword",
        "liquid_volume":"liquid_volume",
        "item_thickness":"item_thickness",
        "air_flow_capacity":"air_flow_capacity",
        "item_depth_width_height_unit_of_measure":"item_depth_width_height",
        "item_length_width_height":"item_length_width_height",
        "pattern":"pattern",
        "material":"material",
        "item_width_height":"item_width_height",
        "item_length_width":"item_length_width",
        "wattage":"wattage",
        "item_depth_width_height":"item_depth_width_height",
        "number_of_pieces":"number_of_pieces",
        "coverage":"coverage",
        "voltage":"voltage",
        "item_diameter":"item_diameter",
        "heater_surface.material":"heater_surface",
        "capacity":"capacity",
        "number_of_items":"number_of_items",
        "maximum_operating_pressure":"maximum_operating_pressure",
        "compatible_phone_models":"compatible_phone_models",
        "bottom_style":"bottom_style",
        "name_of_the_product":"product_name",
        "neck_style":"neck",
        "sun_protection":"sun_protection",    
        "target_audience":"target_audience",
        "scent":"scent",
        "size":"size",
        "count_of_wheels":"number_of_wheels",
        "closure_type":"closure",
        "handle_type":"handle",
        "printer_technology":"printer_technology",
        "printer_output":"printer_output",
        "power/capacity":"capacity",
        "effective_still_resolution":"effective_still_resolution",
    }
    def Merge(dict1, dict2):
        return(dict1.update(dict2))
    Merge(attributeMap,newdictonary)

    attributeList = []
    for i in tqdm(range(len(data))):
        if type(data[i]['MISSING_MANDATORY_ATTRIBUTES']) != float and type(data[i]['MISSING_MANDATORY_ATTRIBUTES']) != int:
            missingAttributes = data[i]['MISSING_MANDATORY_ATTRIBUTES'].replace('|', '').replace('values', '').split(',')
            for k in range(len(missingAttributes)):
                if missingAttributes[k] != '':
                    if missingAttributes[k] not in attributeList:
                        attributeList.append(missingAttributes[k].strip())
    # If there is any printout for this cell then that attribute needs to be mapped with the All Attribute Dump Column Name
    keysleft=[]
    keysList = list(attributeMap.keys())
    for k in range(len(attributeList)):
        if attributeList[k] not in keysList:
            keysleft.append(attributeList[k])
    for i in tqdm(range(len(data))):
        if i > -1:
            for k in range(len(attributeData)):
                if str(data[i]['Asin']).strip() == str(attributeData[k]['ASIN']).strip():
                    asinAttributes = attributeData[k]
                    break
            if type(data[i]['MISSING_MANDATORY_ATTRIBUTES']) != float:
                missingAttributes = str(data[i]['MISSING_MANDATORY_ATTRIBUTES']).replace('|', '').replace('values', '').split(',')
                attr = ""
                for k in range(len(missingAttributes)):
                    column = missingAttributes[k].strip()
                    attributesNA = []
                    if column != '':
                        if attributeMap.get(column) != None:
                            try:
                                attributeColumn = attributeMap[column]
                                if type(asinAttributes[attributeMap[column]]) != float:
                                    attr = attr +  '{}: '.format(column) + str(asinAttributes[attributeMap[column]]) + ', '
                                else:
                                    attributesNA.append(column)
                            except:
                                attributesNA.append(column)
                attr = attr.strip()[:-1]
                data[i]['attr'] = attr
                data[i]['attrNA'] = attributesNA
    for i in tqdm(range(len(data))):
        if data[i].get('attr') != None and str(data[i]['attr']) != 'nan':
            attrs = data[i]['attr'].split('},')
            pAttrs = {}
            for k in range(len(attrs)):
                attr = attrs[k]
                pAttrs[attr.split(':')[0].strip()] = attr.split(':')[-1].replace('"', '').replace('},', '').replace('}', '').strip()
            data[i].update(pAttrs)
    books_df = pd.DataFrame(data)

    with io.BytesIO() as output:
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            books_df.to_excel(writer)
        xceldata = output.getvalue()
    Key='images/Temp_BAU_outfile/amazontitle/{}.xlsx'.format(excelsheetname)
    s3_client.put_object(Bucket=AWS_S3_BUCKET, Key=Key, Body=xceldata)
    outputlink = 'https://inputexcelsheet.s3.ap-south-1.amazonaws.com/'+Key
    return keysleft,outputlink

def input_data(s3link1):
    data = pd.read_excel(s3link1,'input sheet').to_dict('records')
    for i in tqdm(range(len(data))):
        if data[i]['Title'] !=0:
            if str(data[i]['Title']) != "nan":
                data[i]['clean_title'] = ftfy.fix_text(data[i]['Title'])
            else:
                data[i]['clean_title']=" "
        else:
            data[i]['clean_title']=" "
    #unicode Fix - For Title 
    for i in tqdm(range(len(data))):
        if type(data[i]['Title']) == str:
            data[i]['clean_title'] = ftfy.fix_text(data[i]['Title'])
    # Title External URL Fix (@domain)
    for i in tqdm(range(len(data))):
        if str(data[i]['TITLE_EXTERNAL_URL_FOUND']) != '1' and type(data[i].get('clean_title')) == str:
            title = data[i]['clean_title']
            title_split = title.split()
            clean_title_splits = []
            for k in range(len(title_split)):
                if 'www.' not in str(title_split[k]).lower():
                    clean_title_splits.append(title_split[k])
            clean_title = ' '.join(clean_title_splits)
            data[i]['clean_title'] = clean_title


    for i in range(len(data)):
        if str(data[i]['TITLE_KEYWORD_ABUSE']) != '1' and type(data[i].get('clean_title')) == str:
            title = data[i]['clean_title']
            try:
                if re.search(str(data[i]['TITLE_KEYWORD_ABUSE']).split("'")[1].strip().title(), data[i]['clean_title'], re.IGNORECASE):
                    to_replace = re.findall(str(data[i]['TITLE_KEYWORD_ABUSE']).split("'")[1].strip().title(), data[i]['clean_title'], re.IGNORECASE)[0]
                    clean_title = title.replace(to_replace, '').strip()
                    if clean_title[0] == '-' or clean_title[0] == '–':
                        clean_title = clean_title[1:]
                    data[i]['clean_title'] = clean_title.strip()
            except:
                perform = None
    # pd.DataFrame(data).to_excel('clean_title_step_3.xlsx')
    for i in range(len(data)):
        if str(data[i]['TITLE_HAS_BRAND']) != '1' and type(data[i].get('Brand')) == str:
            if data[i]['Brand'].lower() in data[i]['clean_title'].lower() or data[i]['Brand'].lower()[:-1] in data[i]['clean_title'].lower():
                if data[i]['Brand'].lower() in data[i]['clean_title'].lower():
                    brandStartIndex = data[i]['clean_title'].lower().index(data[i]['Brand'].lower())
                    brandEndIndex = brandStartIndex + len(data[i]['Brand'])
                    current_brand = data[i]['clean_title'][brandStartIndex: brandEndIndex]
                    brand_title = (data[i]['clean_title'].replace(current_brand, data[i]['Brand']).strip()).replace('. ', ' ').replace('AmazonBasics', '')
                    data[i]['brand_edited'] = 'TRUE'
                    data[i]['clean_brand_title'] = brand_title.replace('  ', ' ')
                if data[i]['Brand'].lower()[:-1] in data[i]['clean_title'].lower():
                    data[i]['brand_edited'] = 'FALSE - Brand Exists in Title'
            else:
                brand_title = data[i]['Brand'] + ' ' + data[i]['clean_title']
                data[i]['brand_edited'] = 'TRUE'
                data[i]['clean_brand_title'] = brand_title.replace('  ', ' ')
    for i in range(len(data)):
        if type(data[i].get('clean_title')) == str:
            data[i]['clean_title'] = data[i]['clean_title'].replace('  ', ' ')

    return data
