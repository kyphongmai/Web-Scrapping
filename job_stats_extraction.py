from selenium import webdriver
import pandas as pd
import requests
import os
import concurrent.futures

def get_industries():
    browser = webdriver.Chrome('chromedriver-mac-x64/chromedriver')
    browser.get('https://www.seek.com.au/career-advice/')
    linkElem = browser.find_element_by_css_selector('#browseByIndustries > div > div:nth-child(4) > div > div > span > svg')
    linkElem.click()

    industryElem = browser.find_elements_by_css_selector('#browseByIndustries a[href^="/career"]')
    # for item in industryElem:
    #     item_full = item.get_attribute('href')
    #     segment = item_full.replace('https://www.seek.com.au/career-advice/browse/','')
    #     print(segment)
    
    segments = [item.get_attribute('href').replace('https://www.seek.com.au/career-advice/browse/','') for item in industryElem]
    browser.quit()
    return segments

def save_df_csv(data: list, filename: str) -> None:
    os.makedirs(f'data', exist_ok = True)
    path = f'data/{filename}.csv'
    if os.path.exists(path):
        print(f'{path} exists')
    else:
        df = pd.json_normalize(data)
        df.to_csv(path)

def checkExist(filename:str):
    os.makedirs(f'data', exist_ok =True)
    path = f'data/{filename}.csv'
    if os.path.exists(path):
        print(f'{path} exists')
    return os.path.exists(path)

URL = "https://career-guide-api.cloud.seek.com.au/graphql"

HEADERS = {
    "cookie": "sol_id=f43bea5d-d0d6-4861-9d42-5d1ac5fefbca; __cf_bm=lBxdGpNPcsceGw8RWZm6g6yAQvAYe8MJYTxAIBU4zbo-1714911197-1.0.1.1-zoa8mvETkf7UgkKVNpFikyR9LM84wzycb4Tz_j9FZOKOwAS8r8O5eqHiviiJkMmI2aBUlLKtKq4SdPTb3zdZ.Q",
    "accept": "*/*",
    "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
    "apollographql-client-name": "Career advice site",
    "content-type": "application/json",
    "country": "AU",
    "language": "en",
    "origin": "https://www.seek.com.au",
    "priority": "u=1, i",
    "referer": "https://www.seek.com.au/career-advice/browse/information-communication-technology",
    "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "macOS",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-site",
    "seek-request-country": "AU",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "x-seek-site": "skl-career-guide-site"
}

worktype_dict = {242:'Full time', 243:'Part time', 244:'Contract/Temp', 245:'Casual/Vacation'}
workTypes = worktype_dict.keys()
states_AU = ['New South Wales NSW',
            'Victoria VIC',
            'Queensland QLD',
            'South Australia SA',
            'Western Australia WA',
            'Tasmania TAS',
            'Northern Territory NT',
            'Australian Capital Territory ACT']


similarRoles = []
def getSimilarRoles(id:str) -> None:
    payload_similarroles = {
    "operationName": "GetSimilarRoles",
    "variables": {"alias": f"{id}"},
    "query": "query GetSimilarRoles($alias: String!) {\n  roleByAlias(alias: $alias) {\n    id\n    similarRoles {\n      ...RoleCardParts\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment RoleCardParts on Role {\n  id\n  role_id\n  alias\n  url\n  title\n  hero {\n    title\n    __typename\n  }\n  whatsItLike {\n    introduction\n    extraContent\n    videoId\n    imageUrl\n    __typename\n  }\n  roleReviewStatistics {\n    jobSatisfaction\n    __typename\n  }\n  growth {\n    value\n    __typename\n  }\n  salarySuggestion {\n    locationName\n    suggestion\n    salary_min\n    salary_max\n    salary_median\n    __typename\n  }\n  isReleased\n  __typename\n}"
    }

    r_similarRoles = requests.request("POST", URL, json=payload_similarroles, headers=HEADERS)
    similar_list = r_similarRoles.json()['data']['roleByAlias']['similarRoles']
    key_lists = ["role_id"]

    for role in similar_list:
        similarRole_dict = {key: (role.get(key) if role.get(key) is not None else None) for key in key_lists}
        similarRole_dict['primary_alias'] = id
        similarRoles.append(similarRole_dict)

    print(f'Extracting similar roles for {id}')

specReqs = []
def getSpecReqs(id:str)-> None:
    payload_spec = {
        "operationName": "GetRoleSpecReqs",
        "variables": {
            "alias": id,
            "specReqInput": {
                "size": 20,
                "types": ["ReferenceableExperience", "ObservableAttributes", "VerifiableAttainments"]
            }
        },
        "query": "query GetRoleSpecReqs($alias: String!, $specReqInput: RoleSpecReqsInput!) {\n  roleByAlias(alias: $alias) {\n    id\n    specReqs(input: $specReqInput) {\n      id\n      name\n      type\n      label\n      batch\n      __typename\n    }\n    __typename\n  }\n}"
    }
    r_specReq = requests.request("POST", URL, json=payload_spec, headers=HEADERS)
    specReq_list = r_specReq.json()['data']['roleByAlias']['specReqs']
    key_lists = ["type","label"]

    for req in specReq_list:
        specReq_dict = {key: (req.get(key) if req.get(key) is not None else None) for key in key_lists}
        specReq_dict['alias'] = id
        specReqs.append(specReq_dict)

    print(f'Extracting requirements for {id}')

jobCounts = []
URL_search = "https://www.seek.com.au/api/chalice-search/v4/search"

def getJobCounts(title:str):
    for location in states_AU:
        for workTypeCode in workTypes:
            querystring = {"siteKey":"AU-Main","sourcesystem":"houston","userqueryid":"183fc8288e057b95753e44aedc0e138a-9474232",
                        "userid":"efaffe0f-a01b-4cb4-8abe-003741c1c20d","usersessionid":"efaffe0f-a01b-4cb4-8abe-003741c1c20d",
                        "eventCaptureSessionId":"efaffe0f-a01b-4cb4-8abe-003741c1c20d",
                        "where":location,"page":"1","seekSelectAllPages":"true",
                        "keywords":title,
                        "worktype":workTypeCode,
                        "include":"seodata","locale":"en-AU","seekerId":"574629274",
                        "solId":"f43bea5d-d0d6-4861-9d42-5d1ac5fefbca"}
            response = requests.request("GET", URL_search, headers=HEADERS, params=querystring)
            jobCounts_dict = {'title': title, 'location': location, 'work_type': worktype_dict[workTypeCode], 'job counts':response.json()['totalCount']}
            jobCounts.append(jobCounts_dict)
    print(f'Extracting job counts for {title}')

def main():
    industries = get_industries()

    jobs_cleaned =[]
    alias = []
    titles = []
    for industry in industries:
        payload_overview = {
            "operationName": "GetRolesByClassificationUrl",
            "variables": {"segment": f"{industry}"},
            "query": """query GetRolesByClassificationUrl($segment: String) {
                rolesByClassification(segment: $segment) {
                    ...RoleCardParts
                    __typename
                }
                }

                fragment RoleCardParts on Role {
                id
                role_id
                alias
                url
                title
                hero {
                    title
                    __typename
                }
                whatsItLike {
                    introduction
                    extraContent
                    videoId
                    imageUrl
                    __typename
                }
                roleReviewStatistics {
                    jobSatisfaction
                    __typename
                }
                growth {
                    value
                    __typename
                }
                salarySuggestion {
                    locationName
                    suggestion
                    salary_min
                    salary_max
                    salary_median
                    __typename
                }
                isReleased
                __typename
            }"""
        }
        r_jobdata = requests.request("POST", URL, json=payload_overview, headers=HEADERS)
        jobs_list = r_jobdata.json()["data"]["rolesByClassification"]
        key_lists = ["id","role_id","alias","title", "roleReviewStatistics","growth","salarySuggestion" ]

        for job in jobs_list:
            job_dict = {key: (job.get(key) if job.get(key) is not None else None) for key in key_lists}
            job_dict['industry'] = industry
            jobs_cleaned.append(job_dict)
            alias.append(job_dict['alias'])
            titles.append(job_dict['title'])
    
    save_df_csv(data = jobs_cleaned, filename = 'jobOverview')
    
    reviewStats = []
    if not checkExist('reviewStats'):
        for id in alias:
            payload_reviewStats = {
            "operationName": "GetRoleReviewStats",
            "variables": {"alias": f"{id}"},
            "query": "query GetRoleReviewStats($alias: String!) {\n  roleByAlias(alias: $alias) {\n    id\n    roleReviewStatistics {\n      total\n      remuneration\n      employability\n      jobSatisfaction\n      workLifeBalance\n      diversityInTasks\n      careerProgressionOpportunities\n      __typename\n    }\n    __typename\n  }\n}"
            }

            r_reviewdata = requests.request("POST", URL, json=payload_reviewStats, headers=HEADERS)
            review_dict = r_reviewdata.json()["data"]["roleByAlias"]["roleReviewStatistics"]
            if review_dict is not None:
                review_dict['alias'] = id
            else:
                review_dict = {'alias':id}
            reviewStats.append(review_dict)
            print(f'Extracting reviews for {id}')
    
        save_df_csv(data = reviewStats, filename = 'reviewStats')

    if not checkExist('similarRoles'):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(getSimilarRoles, alias)
        
        save_df_csv(data = similarRoles, filename = 'similarRoles')

    if not checkExist('specReq'):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(getSpecReqs, alias)
        
        save_df_csv(data = specReqs, filename = 'specReq')
    
    if not checkExist('jobCounts'):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(getJobCounts, titles)
        
        save_df_csv(data = jobCounts, filename = 'jobCounts')
    
if __name__ == "__main__":
    main()

