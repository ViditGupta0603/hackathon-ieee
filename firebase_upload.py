import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
import hashlib
from datetime import datetime
import json
import os

class FirebaseHackathonUploader:
    def __init__(self, service_account_path=None):
        """
        Initialize Firebase connection
        
        Args:
            service_account_path (str): Path to Firebase service account JSON file
                                      If None, will look for FIREBASE_SERVICE_ACCOUNT_PATH env variable
        """
        self.db = None
        self.initialize_firebase(service_account_path)
    
    def initialize_firebase(self, service_account_path=None):
        """Initialize Firebase Admin SDK"""
        try:
            # Check if Firebase is already initialized
            if firebase_admin._apps:
                self.db = firestore.client()
                print("Using existing Firebase connection")
                return
            
            cred = None
            
            # If env var FIREBASE_SECRET is set, use it instead of file
            firebase_secret_json = os.getenv('FIREBASE_SECRET')
            
            if firebase_secret_json:
                # Parse JSON string from env var
                secret_dict = json.loads(firebase_secret_json)
                cred = credentials.Certificate(secret_dict)
                print("Firebase credential loaded from FIREBASE_SECRET environment variable")
            else:
                # Fall back to file path
                if not service_account_path:
                    service_account_path = os.getenv('FIREBASE_SERVICE_ACCOUNT_PATH')
                
                if not service_account_path:
                    raise ValueError("Firebase service account path not provided. Please provide the path or set FIREBASE_SERVICE_ACCOUNT_PATH environment variable.")
                
                if not os.path.exists(service_account_path):
                    raise FileNotFoundError(f"Service account file not found: {service_account_path}")
                
                cred = credentials.Certificate(service_account_path)
                print(f"Firebase credential loaded from file: {service_account_path}")
            
            firebase_admin.initialize_app(cred)
            self.db = firestore.client()
            print("Firebase initialized successfully")
        
        except Exception as e:
            print(f"Error initializing Firebase: {e}")
            raise

    def create_document_id(self, hackathon_data):
        """Create a unique document ID for each hackathon"""
        title = str(hackathon_data.get("Title", "")).strip().lower()
        org = str(hackathon_data.get("Organisations", "")).strip().lower()
        return hashlib.md5(f"{title}|{org}".encode()).hexdigest()
    
    def clean_data_for_firebase(self, data):
        """Clean data to make it Firebase-compatible"""
        cleaned_data = {}
        
        for key, value in data.items():
            # Replace NaN and None values
            if pd.isna(value) or value is None:
                cleaned_data[key] = None
            elif value == "N/A":
                cleaned_data[key] = None
            else:
                # Convert numpy types to native Python types
                if hasattr(value, 'item'):
                    cleaned_data[key] = value.item()
                else:
                    cleaned_data[key] = value
        
        # Add metadata
        cleaned_data['uploaded_to_firebase'] = datetime.now().isoformat()
        
        return cleaned_data
    
    def upload_hackathons_batch(self, hackathons_data, batch_size=500):
        """Upload hackathons to Firebase in batches"""
        
        if not self.db:
            raise Exception("Firebase not initialized")
        
        total_hackathons = len(hackathons_data)
        print(f"Starting upload of {total_hackathons} hackathons to Firebase...")
        
        added_count = 0
        updated_count = 0
        error_count = 0
        
        # Process in batches
        for i in range(0, total_hackathons, batch_size):
            batch_end = min(i + batch_size, total_hackathons)
            batch_data = hackathons_data[i:batch_end]
            
            print(f"Processing batch {i//batch_size + 1}: records {i+1} to {batch_end}")
            
            # Use Firestore batch
            batch = self.db.batch()
            batch_operations = 0
            
            for _, hackathon in batch_data.iterrows():
                try:
                    # Create document ID
                    doc_id = self.create_document_id(hackathon.to_dict())
                    
                    # Clean data
                    clean_data = self.clean_data_for_firebase(hackathon.to_dict())
                    
                    # Reference to document
                    doc_ref = self.db.collection('hackathons').document(doc_id)
                    
                    # Check if document exists
                    existing_doc = doc_ref.get()
                    
                    if existing_doc.exists:
                        # Update existing document
                        batch.update(doc_ref, clean_data)
                        updated_count += 1
                    else:
                        # Create new document
                        batch.set(doc_ref, clean_data)
                        added_count += 1
                    
                    batch_operations += 1
                    
                    # Commit batch if it reaches Firestore's limit (500 operations)
                    if batch_operations >= 500:
                        batch.commit()
                        batch = self.db.batch()
                        batch_operations = 0
                
                except Exception as e:
                    print(f"Error processing hackathon '{hackathon.get('Title', 'Unknown')}': {e}")
                    error_count += 1
            
            # Commit remaining operations in batch
            if batch_operations > 0:
                try:
                    batch.commit()
                except Exception as e:
                    print(f"Error committing batch: {e}")
                    error_count += batch_operations
        
        print(f"\n=== FIREBASE UPLOAD SUMMARY ===")
        print(f"Total hackathons processed: {total_hackathons}")
        print(f"New documents added: {added_count}")
        print(f"Existing documents updated: {updated_count}")
        print(f"Errors encountered: {error_count}")
        print(f"Successfully uploaded: {added_count + updated_count}")
        
        return {
            'total': total_hackathons,
            'added': added_count,
            'updated': updated_count,
            'errors': error_count
        }
    
    def upload_from_csv(self, csv_file_path='scraped_hackathons.csv', allowed_statuses=None):
        """Upload hackathons from CSV file to Firebase"""
        
        try:
            # Read CSV file
            if not os.path.exists(csv_file_path):
                raise FileNotFoundError(f"CSV file not found: {csv_file_path}")
            
            df = pd.read_csv(csv_file_path, encoding='utf-8')
            print(f"Loaded {len(df)} hackathons from '{csv_file_path}'")
            
            # Filter by status if specified
            if allowed_statuses:
                original_count = len(df)
                df = df[df['Status'].isin(allowed_statuses)]
                filtered_count = len(df)
                print(f"Filtered by status {allowed_statuses}: {filtered_count} hackathons (removed {original_count - filtered_count})")
                
                if filtered_count == 0:
                    print("❌ No hackathons match the specified status criteria!")
                    return {'total': 0, 'added': 0, 'updated': 0, 'errors': 0}
            
            # Remove duplicates (keep the most recent status priority)
            status_priority = {'open': 1, 'recent': 2, 'closed': 3, 'expired': 4}
            df['status_priority'] = df['Status'].map(status_priority).fillna(5)
            
            # Create unique IDs and sort by priority
            df['unique_id'] = df.apply(lambda row: self.create_document_id(row.to_dict()), axis=1)
            df_sorted = df.sort_values('status_priority')
            df_dedup = df_sorted.drop_duplicates(subset=['unique_id'], keep='first')
            df_dedup = df_dedup.drop(['status_priority', 'unique_id'], axis=1)
            
            dedup_removed = len(df) - len(df_dedup)
            if dedup_removed > 0:
                print(f"Removed {dedup_removed} duplicate hackathons (keeping highest priority status)")
            
            print(f"Final hackathons to upload: {len(df_dedup)}")
            
            # Show status distribution
            status_counts = df_dedup['Status'].value_counts()
            print("Status distribution:")
            for status, count in status_counts.items():
                print(f"  - {status}: {count}")
            
            # Upload to Firebase
            result = self.upload_hackathons_batch(df_dedup)
            
            return result
            
        except Exception as e:
            print(f"Error uploading from CSV: {e}")
            raise
    
    def get_hackathons_count(self):
        """Get the current count of hackathons in Firebase"""
        try:
            docs = self.db.collection('hackathons').stream()
            count = sum(1 for _ in docs)
            return count
        except Exception as e:
            print(f"Error getting hackathons count: {e}")
            return None
    
    def delete_hackathons_by_status(self, statuses_to_delete):
        """Delete hackathons with specific statuses from Firebase"""
        try:
            if isinstance(statuses_to_delete, str):
                statuses_to_delete = [statuses_to_delete]
            
            print(f"🗑️  Deleting hackathons with status: {statuses_to_delete}")
            
            # Query hackathons with specified statuses
            deleted_count = 0
            batch = self.db.batch()
            batch_count = 0
            
            for status in statuses_to_delete:
                docs = self.db.collection('hackathons').where('Status', '==', status).stream()
                
                for doc in docs:
                    batch.delete(doc.reference)
                    deleted_count += 1
                    batch_count += 1
                    
                    # Commit batch every 500 operations
                    if batch_count >= 500:
                        batch.commit()
                        batch = self.db.batch()
                        batch_count = 0
                        print(f"Deleted {deleted_count} hackathons so far...")
            
            # Commit remaining deletions
            if batch_count > 0:
                batch.commit()
            
            print(f"✅ Successfully deleted {deleted_count} hackathons with status {statuses_to_delete}")
            return deleted_count
            
        except Exception as e:
            print(f"❌ Error deleting hackathons by status: {e}")
            raise
    
    def delete_all_hackathons(self):
        """Delete all hackathons from Firebase (use with caution!)"""
        try:
            docs = self.db.collection('hackathons').stream()
            batch = self.db.batch()
            count = 0
            
            for doc in docs:
                batch.delete(doc.reference)
                count += 1
                
                # Commit batch every 500 operations
                if count % 500 == 0:
                    batch.commit()
                    batch = self.db.batch()
            
            # Commit remaining deletions
            if count % 500 != 0:
                batch.commit()
            
            print(f"Deleted {count} hackathons from Firebase")
            return count
            
        except Exception as e:
            print(f"Error deleting hackathons: {e}")
            raise

def main():
    SERVICE_ACCOUNT_PATH = None  # Use env variable FIREBASE_SECRET
    CSV_FILE_PATH = "scraped_hackathons.csv"
    ALLOWED_STATUSES = ['open', 'recent']

    try:
        uploader = FirebaseHackathonUploader(SERVICE_ACCOUNT_PATH)
        current_count = uploader.get_hackathons_count()
        if current_count is not None:
            print(f"Current hackathons in Firebase: {current_count}")

        print("\n🧹 Cleaning up expired hackathons from Firebase...")
        uploader.delete_hackathons_by_status(['expired'])

        result = uploader.upload_from_csv(CSV_FILE_PATH, allowed_statuses=ALLOWED_STATUSES)

        final_count = uploader.get_hackathons_count()
        if final_count is not None:
            print(f"Final hackathons in Firebase: {final_count}")

    except Exception as e:
        print(f"Upload failed: {e}")

if __name__ == "__main__":
    main()