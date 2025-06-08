import hackathon_scrape
import firebase_upload

def main():
    hackathon_scrape.scrape_hackathons()       # Assuming you have a function like this
    firebase_upload.main()   # And one like this too

if __name__ == "__main__":
    main()
