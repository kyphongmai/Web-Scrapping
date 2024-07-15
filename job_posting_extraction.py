import requests
import pandas as pd
import concurrent.futures

url = "https://www.seek.com.au/api/chalice-search/v4/search"

jobs_cleaned = []

def collect_data(PAGE):
    querystring = {"siteKey":"AU-Main","sourcesystem":"houston","userqueryid":"c52a948a38f63c79debe8f8efcdc5897-6463724",
                   "userid":"efaffe0f-a01b-4cb4-8abe-003741c1c20d","usersessionid":"efaffe0f-a01b-4cb4-8abe-003741c1c20d",
                   "eventCaptureSessionId":"efaffe0f-a01b-4cb4-8abe-003741c1c20d","where":"All Australia",
                   "page":f'{PAGE}',"seekSelectAllPages":"true","keywords":"data","sortmode":"KeywordRelevance",
                   "include":"seodata","locale":"en-AU","seekerId":"574629274","solId":"f43bea5d-d0d6-4861-9d42-5d1ac5fefbca"}
    payload = ""
    headers = {
        "cookie": "JobseekerSessionId=efaffe0f-a01b-4cb4-8abe-003741c1c20d; JobseekerVisitorId=efaffe0f-a01b-4cb4-8abe-003741c1c20d; sol_id=f43bea5d-d0d6-4861-9d42-5d1ac5fefbca; last-known-sol-user-id=a1fb93ae2c541b00031f4ad692a460f4dab7b24fc4c50f140e4f3333edf4fa75b6fef5172b99e2f502d578fed06c3560fa9e41bcace8899efca48b267c8f67c661f87f29a0a3ed212007f365a3e9f8ff27a1cd67cd92a2b27382dff56df38c874af8b93c6112f6917659cc7d094a5d1ec21ee08f7f4cb1fedca6e963d195061a4fc7d1442304bb932e60dcc5a878629a71c632dbe6274368196988f25c8b7d5ef09fb6c391ac5a; da_cdt=visid_018d5f712d7c0011a755291ba1ec05075003806d01788-sesid_1714872216833; _legacy_auth0.yGBVge66K5NJpSN5u71fU90VcTlEASNu.is.authenticated=true; auth0.yGBVge66K5NJpSN5u71fU90VcTlEASNu.is.authenticated=true; da_anz_candi_sid=1714872216833; da_searchTerm=undefined; __cf_bm=OCKOCAiNBHDVJ6uhZOXoBu8GnIqCH95BNPmB7YpgRpA-1714875697-1.0.1.1-Hffs4Hy4Rw2dyOdOoI8efNbEE0tOPfqiiDtM7phjupvMMA_i0sKV7vuzwx.B.Zgz6wVizzQ9WR5myov6ssqe_Q; main=V%7C2~P%7Cjobsearch~K%7Cdata~WID%7C3000~SORT%7CKeywordRelevance~W%7C242~L%7C3000~OSF%7Cquick&set=1714875720242/V%7C2~P%7Cjobsearch~K%7CStatistician~WID%7C3000~L%7C3000~OSF%7Cquick&set=1714872312234/V%7C2~P%7Cjobsearch~K%7CMachine%20Learning%20Engineer~WID%7C3000~L%7C3000~OSF%7Cquick&set=1714872295134; utag_main=v_id:018d5f712d7c0011a755291ba1ec05075003806d01788$_sn:18$_se:42%3Bexp-session$_ss:0%3Bexp-session$_st:1714877521263%3Bexp-session$dc_visit:8$ses_id:1714872216833%3Bexp-session$_pn:6%3Bexp-session$dc_event:92%3Bexp-session$krux_sync_session:1714872216833%3Bexp-session$_prevpage:search%20results%3Bexp-1714879321273; _dd_s=rum=0&expire=1714877363350",
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "priority": "u=1, i",
        "referer": "https://www.seek.com.au/data-jobs?sortmode=KeywordRelevance",
        "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "seek-request-brand": "seek",
        "seek-request-country": "AU",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "x-seek-checksum": "12bb5de9",
        "x-seek-site": "Chalice"
    }

    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)

    jobs_list = response.json()["data"]
    key_lists = ['title', 'classification','subClassification','roleId', 'companyName','advertiser', 
                 'salary','workType','isPrivateAdvertiser', 'listingDate', 'isPremium', 'isStandOut',
                  'suburb','suburbWhereValue']
    
    for job in jobs_list:
        job_dict = {key: (job.get(key) if job.get(key) is not None else None) for key in key_lists}
        jobs_cleaned.append(job_dict)
    #print(len(jobs_list))

def main():
    pages = range(1,100)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(collect_data, pages)
    df = pd.json_normalize(jobs_cleaned)
    df.to_csv('data/jobpostings_result.csv')
    

if __name__ == "__main__":
    main()