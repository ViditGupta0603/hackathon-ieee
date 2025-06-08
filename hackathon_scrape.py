import requests
import time
import pandas as pd
from datetime import datetime

# Define headers for the request
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.6613.138 Safari/537.36",
    "Accept-Language": "en-GB,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
}

def days_in_month(date_str):
    try:
        date_obj = datetime.fromisoformat(date_str)
    except ValueError:
        print(f"Date format error: {date_str}")
        return "N/A"
    
    if date_obj.month == 2:
        is_leap = date_obj.year % 4 == 0 and (date_obj.year % 100 != 0 or date_obj.year % 400 == 0)
        return 29 if is_leap else 28
    
    days_in_months = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    return days_in_months[date_obj.month - 1]

def calculate_deadline(durations, text, start_date):
    try:
        durations = int(durations)
    except (ValueError, TypeError):
        print(f"Invalid duration value: {durations}")
        return "N/A"

    if "day" in text.lower():
        return durations
    elif "month" in text.lower():
        days_in_this_month = days_in_month(start_date)
        if isinstance(days_in_this_month, int):
            return durations * days_in_this_month
    return "N/A"

def extract_page(url, params):
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None

def scrape_hackathons():
    base_url = "https://unstop.com/api/public/opportunity/search-result"
    oppstatuses = ["open", "closed", "recent", "expired"]
    hackathons_per_page = 30
    MAX_PAGES = 20

    all_hackathons = []

    for status in oppstatuses:
        print(f"\nScraping hackathons with oppstatus='{status}'")
        search_params = {
            "opportunity": "hackathons",
            "oppstatus": status,
            "page": 1,
            "size": hackathons_per_page
        }

        headers["Referer"] = f"https://unstop.com/api/public/opportunity/search-result?opportunity=hackathons&oppstatus={status}"

        current_page = 1
        while current_page <= MAX_PAGES:
            print(f"Scraping page {current_page} for status='{status}'...")
            data = extract_page(base_url, search_params)

            if data and "data" in data and "data" in data["data"]:
                hackathons = data["data"]["data"]
                if not hackathons:
                    print(f"No more hackathons found on page {current_page} for status='{status}'.")
                    break

                for hackathon in hackathons:
                    organisation = hackathon.get("organisation")
                    organisation_name = organisation.get("name", "N/A") if organisation else "N/A"
                    applied = hackathon.get("registerCount", "N/A")
                    impressions = hackathon.get("viewsCount", "N/A")

                    filters = hackathon.get("filters", [])
                    eligibility_list = [f.get("name", "N/A") for f in filters]
                    eligibility = ", ".join(eligibility_list) if eligibility_list else "N/A"
                    category = eligibility  # same as eligibility

                    regn_requirements = hackathon.get("regnRequirements", {})
                    remaining_days_array = regn_requirements.get("remainingDaysArray", {})
                    duration = remaining_days_array.get("durations", 0)
                    text = remaining_days_array.get("text", "")
                    start_date = hackathon.get("start_date", "N/A")

                    application_deadline = calculate_deadline(duration, text, start_date) if start_date != "N/A" else "N/A"

                    hackathon_entry = {
                        "Title": hackathon.get("title", "N/A"),
                        "Organisations": organisation_name,
                        "Link": f"https://unstop.com/{hackathon.get('public_url', '')}",
                        "Uploaded On": start_date,
                        "Opportunity Type": hackathon.get("type", "N/A"),
                        "Status": status,
                        "Applied": applied,
                        "Application Deadline": application_deadline,
                        "Impressions": impressions,
                        "Eligibility": eligibility,
                        "Category": category,
                        "Region": hackathon.get("region", "N/A")
                    }

                    all_hackathons.append(hackathon_entry)

                current_page += 1
                search_params["page"] = current_page
                time.sleep(1)
            else:
                print("Invalid response structure or failed to retrieve data.")
                break

        print(f"Completed scraping up to {min(current_page - 1, MAX_PAGES)} pages for status='{status}'.")

    print(f"\nTotal hackathons scraped: {len(all_hackathons)}")

    df = pd.DataFrame(all_hackathons)
    csv_filename = 'scraped_hackathons.csv'
    df.to_csv(csv_filename, index=False, encoding='utf-8')
    print(f"Data saved to '{csv_filename}'")

    return df

if __name__ == "__main__":
    scrape_hackathons()
