import tls_client
import json
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from save_bd import Tickets, NewTickets
from bs4 import BeautifulSoup
from config import engine
def login() -> tls_client.Session: 
    s = tls_client.Session(client_identifier='chrome_105')

    json_data = {
        'email': 'vitaliytravels@gmail.com',
        'password': 'Test1234567890',
        'rememberMe': True,
    }
    s.post('https://apiv2.thriftytraveler.com/auth/login', json=json_data)
    return s
def get_data():
    s = login()
    data = []
    params = {
        'order': 'DESC',
        'orderBy': 'createdAt',
        'search': '',
        'departureCities': [
            'Aberdeen (ABR)', 'Abilene (ABI)', 'Akron (CAK)', 'Albany (ALB)', 'Albuquerque (ABQ)',
            'Allentown (ABE)', 'Anchorage (ANC)', 'Appleton (ATW)', 'Arcata–Eureka (ACV)', 'Augusta (AGS)',
            'Austin (AUS)', 'Asheville (AVL)', 'Atlanta (ATL)', 'Bakersfield (BFL)', 'Baltimore (BWI)',
            'Bangor (BGR)', 'Baton Rouge (BTR)', 'Bemidji (BJI)', 'Billings (BIL)', 'Birmingham (BHM)',
            'Bismarck (BIS)', 'Bloomington (BMI)', 'Boise (BOI)', 'Boston (BOS)', 'Bozeman (BZN)',
            'Brainerd (BRD)', 'Bristol (TRI)', 'Buffalo (BUF)', 'Burbank (BUR)', 'Burlington (BTV)',
            'Butte (BTM)', 'Calgary (YYC)', 'Cedar Rapids (CID)', 'Charleston (CHS)', 'Charleston (CRW)',
            'Charlotte (CLT)', 'Charlottesville (CHO)', 'Chattanooga (CHA)', 'Chicago (ORD)', 'Chicago (MDW)',
            'Cincinnati (CVG)', 'Cleveland (CLE)', 'Colorado Springs (COS)', 'Columbia (CAE)', 'Columbia (COU)',
            'Columbus (CMH)', 'Columbus (GTR)', 'Corpus Christi (CRP)', 'Dallas (DFW)', 'Dallas (DAL)',
            'Dayton (DAY)', 'Daytona Beach (DAB)', 'Denver (DEN)', 'Devils Lake (DVL)', 'Destin (VPS)',
            'Detroit (DTW)', 'Des Moines (DSM)', 'Dickinson (DIK)', 'Dothan (DHN)', 'Dubuque (DBQ)',
            'Duluth (DLH)', 'Eau Claire (EAU)', 'Edmonton (YEG)', 'El Paso (ELP)', 'Erie (ERI)', 'Eugene (EUG)',
            'Evansville (EVV)', 'Fairbanks (FAI)', 'Fargo (FAR)', 'Fayetteville (FAY)', 'Fayetteville (XNA)',
            'Flint (FNT)', 'Fort Lauderdale (FLL)', 'Fort Myers (RSW)', 'Fort Wayne (FWA)', 'Fresno (FAT)',
            'Gainesville (GNV)', 'Grand Forks (GFK)', 'Grand Junction (GJT)', 'Grand Rapids (GRR)',
            'Great Falls (GTF)', 'Green Bay (GRB)', 'Greensboro (GSO)', 'Greenville (GSP)', 'Gulfport (GPT)',
            'Harrisburg (MDT)', 'Hartford (BDL)', 'Hibbing (HIB)', 'Honolulu (HNL)', 'Houston (IAH)', 'Houston (HOU)',
            'Huntsville (HSV)', 'Idaho Falls (IDA)', 'Indianapolis (IND)', 'International Falls (INL)', 'Jackson (JAN)',
            'Jackson Hole (JAC)', 'Jacksonville (JAX)', 'Jamestown (JMS)', 'Joplin (JLN)', 'Juneau (JNU)', 'Kalamazoo (AZO)',
            'Kalispell (FCA)', 'Kansas City (MCI)', 'Key West (EYW)', 'Knoxville (TYS)', 'Kona (KOA)', 'La Crosse (LSE)',
            'Lafayette (LFT)', 'Lansing (LAN)', 'Las Vegas (LAS)', 'Lexington (LEX)', 'Little Rock (LIT)', 'Lincoln (LNK)',
            'Louisville (SDF)', 'Long Beach (LGB)', 'Los Angeles (LAX)', 'Lubbock (LBB)', 'Madison (MSN)',
            'Manchester–Boston (MHT)', 'McAllen (MFE)', 'Medford (MFR)', 'Melbourne (MLB)', 'Memphis (MEM)', 'Miami (MIA)',
            'Midland (MAF)', 'Milwaukee (MKE)', 'Minneapolis (MSP)', 'Minot (MOT)', 'Missoula (MSO)', 'Mobile (MOB)',
            'Moline (MLI)', 'Monterey (MRY)', 'Montgomery (MGM)', 'Montreal (YUL)', 'Montrose (MTJ)', 'Myrtle Beach (MYR)',
            'Nashville (BNA)', 'New Orleans (MSY)', 'New York (JFK)', 'New York (LGA)', 'Newark (EWR)', 'Norfolk (ORF)',
            'Oakland (OAK)', 'Oklahoma City (OKC)', 'Omaha (OMA)', 'Ontario (ONT)', 'Orlando (MCO)', 'Ottawa (YOW)',
            'Palm Springs (PSP)', 'Panama City (ECP)', 'Pasco (PSC)', 'Pensacola (PNS)', 'Peoria (PIA)', 'Philadelphia (PHL)',
            'Phoenix (PHX)', 'Pittsburgh (PIT)', 'Pocatello (PIH)', 'Portland (PDX)', 'Portland (PWM)', 'Providence (PVD)',
            'Quebec City (YQB)', 'Raleigh (RDU)', 'Rapid City (RAP)', 'Redding (RDD)', 'Redmond (RDM)', 'Regina (YQR)',
            'Reno (RNO)', 'Rhinelander (RHI)', 'Richmond (RIC)', 'Roanoke (ROA)', 'Rochester (ROC)', 'Rochester (RST)',
            'Salt Lake City (SLC)', 'San Antonio (SAT)', 'Sacramento (SMF)', 'San Diego (SAN)', 'San Jose (SJC)', 'San Juan (SJU)',
            'San Francisco (SFO)', 'San Luis Obispo (SBP)', 'Santa Ana (SNA)', 'Santa Barbara (SBA)', 'Santa Fe (SAF)', 'Sarasota (SRQ)',
            'Saskatoon (YXE)', 'Savannah (SAV)', 'Scranton (AVP)', 'Seattle (SEA)', 'Shreveport (SHV)', 'Sioux City (SUX)',
            'Sioux Falls (FSD)', 'South Bend (SBN)', 'Spokane (GEG)', 'Springfield (SGF)', 'Springfield (SPI)', 'St. Louis (STL)',
            'St. Thomas (STT)', 'Steamboat Springs (HDN)', 'Syracuse (SYR)', 'Tallahassee (TLH)', 'Tampa (TPA)', 'Thunder Bay (YQT)',
            'Toledo (TOL)', 'Toronto (YYZ)', 'Traverse City (TVC)', 'Tucson (TUS)', 'Tulsa (TUL)', 'Vancouver (YVR)', 'Victoria (YYJ)',
            'Waco (ACT)', 'Washington, D.C. (DCA)', 'Washington, D.C. (IAD)', 'Watertown (ATY)', 'Westchester County (HPN)',
            'Wichita (ICT)', 'Wichita Falls (SPS)', 'Williston (XWA)', 'Wilmington (ILM)', 'Winnipeg (YWG)', 'Wausau (CWA)',
            'West Palm Beach (PBI)'
        ],
        'cabin[]': ['BUSINESS_AND_FIRST_CLASS', 'ECONOMY'],
        'destinationType[]': ['DOMESTIC', 'INTERNATIONAL'],
        'priceMin': 0,
        'priceMax': 3000,
        'pointMin': 0,
        'pointMax': 200,
        'status': 'PUBLISHED',
        'page': 1
    }

    r = s.get('https://apiv2.thriftytraveler.com/deals', params=params)
    page_count = r.json()['meta']['totalPages']
    with open('test.json', 'w') as f:
        json.dump(r.json(),f,indent=2)
    for page in range(1, int(page_count) + 1):
        params['page'] = page
        r = s.get('https://apiv2.thriftytraveler.com/deals', params=params)
        for item in r.json()['items']:
            type = item['type']
            cabin = item['cabin']
            title = item['title']
            if 'points' in type.lower():
                type = 'Points'
                price = f"{item['pricePrefix']} {item['price']}k points"
            else:
                type = 'Cash'
                price = f"{item['pricePrefix']} {item['price']}$"
            bookURL = item['bookUrl']
            symbol = '$' if 'CASH' in type else ' Points'
            departure_cities = '\n'.join([f"{i['destination']}: {i['price']}{symbol}" for i in item["departureCities"]])
            soup = BeautifulSoup(item["departureCitiesContent"].replace('<br>', '\n'), 'lxml')
            # departure_cities = soup.text.strip()
            departure_cities = []
            for i in soup.find_all('h2'):
                if i.find('strong'):
                    strong = i.find("strong").text.strip()
                    # print(strong)
                    normal = i.text.replace(f"{strong}", "").replace("<br>", "\n").replace('\r', '').strip()
                    msg = f'\n<b>{strong}</b>\n{normal}'
                    departure_cities.append(msg)
                else:departure_cities.append(i.text.strip())
            departure_cities = '\n'.join(departure_cities)
            departure_airports = ', '.join([i['city'] for i in item['departureCities']])
            # departure_cities = item["departureCitiesContent"].replace('<br>', '\n').replace('h2', 'b').replace('<p>', '\n').replace('<\p>', '\n').replace('<hr>', '').replace('</hr>', '-----------')
            id = item['id']
            data.append({
                # departure_cities, 
                'ID' : id,
                'Title': title, 
                'Type': type, 
                'Cabin': cabin,
                'Price': price, 
                'Book' : bookURL,
                'DepartureCities' : departure_cities,
                'DepartureAirports' : departure_airports})
    return data

def add_db() -> bool:
    # current_datetime = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M')
    data = get_data()

    Session = sessionmaker(bind=engine)
    session = Session()
    for item in data:
        exist = session.query(Tickets).filter_by(ID = item['ID']).first()
        if not exist:
            session.add(Tickets(**item))
            session.add(NewTickets(**item))
    session.commit()
    count = session.query(NewTickets).count()
    session.close()
    return count > 0

        
if __name__ == '__main__':
    add_db()