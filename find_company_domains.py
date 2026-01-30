import csv
import time
from linkup import LinkupClient

client = LinkupClient(api_key="618ccb05-0186-4e66-9226-208943cd0126")

# Read company names from CSV
csv_file = "Scrapping job offer - thetruckersreport.com.csv"
companies = set()

with open(csv_file, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        name = row["Company Name"].strip()
        if name:
            companies.add(name)

companies = sorted(companies)[:10]  # Limit to first 10 companies
print(f"Processing {len(companies)} companies.\n")

# Output CSV
output_file = "company_domains.csv"
with open(output_file, "w", newline="", encoding="utf-8") as out:
    writer = csv.writer(out)
    writer.writerow(["Company Name", "Official Domain"])

    # Process in batches of 10 to reduce API calls
    batch_size = 10
    for i in range(0, len(companies), batch_size):
        batch = companies[i : i + batch_size]
        company_list = "\n".join(f"- {c}" for c in batch)

        query = (
            "You are a data research assistant. Given this list of trucking/transportation company names, "
            "identify and return the official website domain for each company. "
            "Focus on finding the primary corporate domain (not subsidiaries or unrelated sites). "
            "Return ONLY a simple list in the format: Company Name | domain.com\n"
            "If you cannot find a domain, write 'NOT FOUND'.\n\n"
            f"Companies:\n{company_list}"
        )

        print(f"Processing batch {i // batch_size + 1}/{(len(companies) + batch_size - 1) // batch_size}...")

        try:
            response = client.search(
                query=query,
                depth="standard",
                output_type="sourcedAnswer",
                include_images=False,
                include_inline_citations=False,
            )

            # Parse response
            text = response.answer if hasattr(response, "answer") else str(response)
            print(text)
            print("---")

            # Try to parse "Company | domain" lines
            for line in text.split("\n"):
                line = line.strip().strip("-").strip("*").strip()
                if "|" in line:
                    parts = line.split("|", 1)
                    company_name = parts[0].strip()
                    domain = parts[1].strip()
                    writer.writerow([company_name, domain])

        except Exception as e:
            print(f"Error processing batch: {e}")
            for c in batch:
                writer.writerow([c, "ERROR"])

        time.sleep(1)  # Rate limiting

print(f"\nDone! Results saved to {output_file}")
