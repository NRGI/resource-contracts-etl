import parser
import os
from bs4 import BeautifulSoup
import requests
import pycountry
from datetime import datetime
from time import strftime
import time
import json
import csv
import logging
LOGGER = logging.getLogger()
logging.basicConfig(level=logging.INFO)
START_TIME = time.time()

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

import pprint
PP = pprint.PrettyPrinter(indent=4)

# # data model
# [
#     {
#         country_ID : string - 2 char <ISO2>,
#         num_contracts : int <number of contracts at source>,
#         oo_plain_index_url : url <url for plain text index of files>,
#         oo_wiki_index_url : url <url for OO wiki country index page>,
#         zip_url : url <url for zip file containing all contracts for that country>,
#         contract data : [
#             {
#                 contract_contractor : string,
#                 contract_disclosure: string <government, corporate, informal or found elsewhere, NULL if missing>,
#                 contract_host_gov_contract : string (maybe can map to bool),
#                 contract_language : string,
#                 contract_link : url <link to pdf version of file>,
#                 contract_oo_index_url : url <url for OO wiki contract index page>,
#                 contract_signed_date : date <ISO format, NULL if missing>,
#                 contract_source : url <link to where pdf was originall pulled from, NULL if missing>,,
#                 contract_summary : url <link to summary of contract (RC.ORG), NULL if missing>,
#                 contract_title : string,
#                 contract_title_type : string
#             }...
#         ]
#     }...
# ]


def getContractExtraMeta(data, url):
    '''Get OO contract data on disclosure mode and ultimate source. If not exist set to NULL'''
    contract_url = 'http://repository.openoil.net' + url
    contract_req = requests.get(contract_url)
    contract_cont = contract_req.content
    contract_soup = BeautifulSoup(contract_cont, 'html.parser')
    contract_table = contract_soup.find('table', {'class' : 'wikitable'})
    contract_rows = contract_table.findAll('tr')
    try:
        data['contract_source'] = contract_rows[6].td.a.attrs['href']
        data['contract_disclosure'] = contract_rows[7].td.string.encode('utf-8').strip().lower()
    except AttributeError:
        data['contract_source'] = 'NULL'
        data['contract_disclosure'] = 'NULL'
    except:
        raise

def getContract(contract_url):
    '''Get OO contract from source and store in local repository with chunking for large files'''
    local_filename = contract_url.split('/')[-1]
    if not os.path.exists(CONTRACT_REPO + '/' + local_filename):
        LOGGER.info('Retrieving contract %s' % (local_filename))
        contract_req = requests.get(contract_url)
        contract_file = open(CONTRACT_REPO + '/' + local_filename, 'wb')
        # # to set up sub dirs need to pass in country ID
        # if not os.path.exists(CONTRACT_REPO + '/' + <country_ID>):
        #     os.makedirs(CONTRACT_REPO + '/' + <country_ID>)
        # f = open(CONTRACT_REPO + '/' + <country_ID> + '/' + local_filename, 'wb')
        for chunk in contract_req.iter_content(chunk_size=512 * 1024): 
            if chunk: # filter out keep-alive new chunks
                contract_file.write(chunk)
        contract_file.close()
        elapsed_time = time.time() - START_TIME
        LOGGER.info('Retrieved %s and stored in repo (elapsed time - %d seconds)' % (local_filename, elapsed_time))
    else:
        print bcolors.OKGREEN + 'Contract %s already exists, moving on...' % (local_filename) + bcolors.ENDC
    return
    


def getCountryMeta(data, url):
    '''Scrape Open Oil country page contract tables for contract index and metadata as well
    as to get link to actual contract'''
    # url of country page
    country_url = 'http://repository.openoil.net' + url
    # get and parse html
    country_req = requests.get(country_url)
    country_cont = country_req.content
    country_soup = BeautifulSoup(country_cont, 'html.parser')
    # find table with data and pass through rows
    country_table = country_soup.find('table', {'class' : 'wikitable sortable'})
    country_rows = country_table.findAll('tr')
    for country_row in country_rows:
        # initiate data and get cells of row
        country_row_data = {}
        country_cells = country_row.findAll('td')
        if len(country_cells) > 1:
            # contract title
            country_row_data['contract_title'] = country_cells[0].a.string.strip().encode('utf-8')
            # contract link
            country_row_data['contract_oo_index_url'] = country_cells[0].a.attrs['href'].encode('utf-8')
            # contract title type
            country_row_data['contract_title_type'] = country_cells[1].string.strip().encode('utf-8')
            # contract contractor
            country_row_data['contract_contractor'] = country_cells[2].string.strip().encode('utf-8')
            # contract signed formateed as ISO with NULL if missing
            try:
                country_row_data['contract_signed_date'] = datetime.strptime(country_cells[3].string.strip(), '%B %d, %Y').isoformat()
            except ValueError:
                try:
                    country_row_data['contract_signed_date'] = datetime.strptime(country_cells[3].string.strip(), '%B %d. %Y').isoformat()
                except ValueError:
                    try:
                        country_row_data['contract_signed_date'] = datetime.strptime(country_cells[3].string.strip(), '%Y').isoformat()
                    except:
                        if country_cells[3].string.strip() == '2 0 2011':
                            country_row_data['contract_signed_date'] = datetime.strptime('February 2011', '%B %Y').isoformat()
                        else:
                            country_row_data['contract_signed_date'] = 'NULL'
            except:
                raise
            # contract host gov contract
            country_row_data['contract_host_gov_contract'] = country_cells[4].string.strip().encode('utf-8')
            # contract language
            country_row_data['contract_language'] = country_cells[5].string.strip().encode('utf-8')
            # contract summary link, set to NULL if missing
            try:
                country_row_data['contract_summary'] = country_cells[6].a.attrs['href'].encode('utf-8')
            except AttributeError:
                country_row_data['contract_summary'] = 'NULL'
            # link to actual contract
            country_row_data['contract_link'] = country_cells[7].a.attrs['href'].encode('utf-8')
        if len(country_row_data) != 0:
            # download contract
            getContract(country_row_data['contract_link'])
            # extract source and disclosure data
            getContractExtraMeta(country_row_data, country_row_data['contract_oo_index_url'])

            data['contract_data'].append(country_row_data)
    elapsed_time = time.time() - START_TIME
    LOGGER.info('Contract metadata retrieved, moving on (elapsed time - %d seconds)' % (elapsed_time))


def getCountryList(data, out_csv, url):
    '''Scrape Open Oil contract repo front page table of country contract repositories
    for index pages and basic country metadata'''
   # Get seed list of pages to parse
    main_req = requests.get(url)
    main_cont = main_req.content
    main_soup = BeautifulSoup(main_cont, 'html.parser')
    # concession data can be found at the main simple wikitable
    main_table = main_soup.find('table', {'class' : 'wikitable sortable'})
    # get rows of table and iterate though
    main_rows = main_table.findAll('tr')
    for main_row in main_rows:
        # create row object to insert into data array
        main_row_data = {}
        # get cells of row and look iterate trhough
        main_cells = main_row.findAll('td')
        # pass by blank rows
        if len(main_cells) > 1:
            try:
                LOGGER.info('Retrieving metadata for %s' % (main_cells[0].a.attrs['title']))
            except AttributeError:
                pass
            try:
                main_row_data['country_ID'] = pycountry.countries.get(name=main_cells[0].a.attrs['title']).alpha2.encode('utf-8')
                # URL of OO country index page
                main_row_data['oo_wiki_index_url'] = main_cells[0].a.attrs['href'].encode('utf-8')
            # Error catching for non-conforming country names
            except KeyError:
                if main_cells[0].a.attrs['title'] == 'Bolivia':
                    main_row_data['country_ID'] = 'BO'
                    main_row_data['oo_wiki_index_url'] = '/wiki/Bolivia'
                elif main_cells[0].a.attrs['title'] == 'Brunei':
                    main_row_data['country_ID'] = 'BN'
                    main_row_data['oo_wiki_index_url'] = '/wiki/Brunei'
                elif main_cells[0].a.attrs['title'] == 'Democratic Republic of Congo':
                    main_row_data['country_ID'] = 'CD'
                    main_row_data['oo_wiki_index_url'] = '/wiki/Democratic_Republic_of_Congo'
                elif main_cells[0].a.attrs['title'] == 'Iraqi Kurdistan':
                    main_row_data['country_ID'] = 'XI'
                    main_row_data['oo_wiki_index_url'] = '/wiki/Iraqi_Kurdistan'
                elif main_cells[0].a.attrs['title'] == 'Syria':
                    main_row_data['country_ID'] = 'SY'
                    main_row_data['oo_wiki_index_url'] = '/wiki/Syria'
                elif main_cells[0].a.attrs['title'] == 'Tanzania':
                    main_row_data['country_ID'] = 'TZ'
                    main_row_data['oo_wiki_index_url'] = '/wiki/Tanzania'
                elif main_cells[0].a.attrs['title'] == 'Venezuela':
                    main_row_data['country_ID'] = 'VE'
                    main_row_data['oo_wiki_index_url'] = '/wiki/Venezuela'
                else:
                    raise
            except AttributeError:
                pass
            # number of contracts
            try:
                main_row_data['num_contracts'] = int(main_cells[1].string.strip().encode('utf-8'))
            except ValueError:
                pass
            # Zip file URL
            try:
                main_row_data['zip_url'] = main_cells[2].a.attrs['href'].encode('utf-8')
            except AttributeError:
                pass
            # country index page
            try:
                main_row_data['oo_plain_index_url'] = main_cells[3].a.attrs['href']
            except AttributeError:
                pass

        # pass by empty rows
        if len(main_row_data) != 0:
            # Initiate contract data
            main_row_data['contract_data'] = []
            # get data from country index page
            getCountryMeta(main_row_data, main_row_data['oo_wiki_index_url'])
            # add to record
            data.append(main_row_data)
            for csv_contract in main_row_data['contract_data']:
                csv_array = []
                csv_array.append(csv_contract['contract_title'])
                csv_array.append(csv_contract['contract_title_type'])
                csv_array.append(csv_contract['contract_contractor'])
                csv_array.append(csv_contract['contract_link'])
                csv_array.append(main_row_data['country_ID'])
                csv_array.append('')
                csv_array.append(csv_contract['contract_signed_date'])
                csv_array.append(csv_contract['contract_language'])
                csv_array.append(csv_contract['contract_disclosure'])
                csv_array.append(csv_contract['contract_host_gov_contract'])
                out_csv.append(csv_array)

            # # # Test on single Country
            # if main_row_data['country_ID'] == 'AL':
            #     main_row_data['contract_data'] = []
            #     getCountryMeta(main_row_data, main_row_data['oo_wiki_index_url'])
            #     for csv_contract in main_row_data['contract_data']:
            #         csv_array = []
            #         csv_array.append(csv_contract['contract_title'])
            #         csv_array.append(csv_contract['contract_title_type'])
            #         csv_array.append(csv_contract['contract_contractor'])
            #         csv_array.append(csv_contract['contract_link'])
            #         csv_array.append(main_row_data['country_ID'])
            #         csv_array.append('')
            #         csv_array.append(csv_contract['contract_signed_date'])
            #         csv_array.append(csv_contract['contract_language'])
            #         csv_array.append(csv_contract['contract_disclosure'])
            #         csv_array.append(csv_contract['contract_host_gov_contract'])
            #         out_csv.append(csv_array)
                    
            #     PP.pprint(main_row_data)
            #     data.append(main_row_data)

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    
    import argparse
    parser = argparse.ArgumentParser(
        description='download and update Open Oil contract list and urls')

     # Output file option
    parser.add_argument('-o', '--output', dest='filepath', action='store',
                        default='.', metavar='filepath',
                        help='define output filepath')
    # Source file (default is the global cpi_source)
    parser.add_argument('-s', '--source', dest='source', action='store',\
                        default='http://repository.openoil.net/wiki/Downloads',\
                        metavar='source', help='define source')

    # Parse the arguments into ARGS
    ARGS = parser.parse_args()

    # set paths
    SOURCE_URL = ARGS.source
    DEST_PATH = ARGS.filepath
    DEST_FILE = DEST_PATH + '/contract_data.json'
    CONTRACT_REPO = DEST_PATH + '/contracts'

    # make repo dir if not exist
    if not os.path.exists(CONTRACT_REPO):
        os.makedirs(CONTRACT_REPO)

    # initiate data object
    data = []
    # Initiate out csv
    out_csv = [['Contract','Title','Company','PDF URL','Country','Concession/License name','Signature date','Language','Disclosure mode', 'Host gov contract']]
    # parse website
    getCountryList(data, out_csv, SOURCE_URL)
    # write out data
    LOGGER.info('Writing data to %s' % (DEST_FILE))
    with open(DEST_PATH + '/contract_csv.tsv', 'w') as outfile_csv:
        csv_writer = csv.writer(outfile_csv, delimiter='	', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in out_csv:
            csv_writer.writerow(row)
    
    with open(DEST_FILE, 'w') as outfile:
        outfile.write(json.dumps(data))
    LOGGER.info('Contracts and metadata retrieved - %d seconds' % (time.time() - START_TIME))