from selenium import webdriver
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup as bs
import time
import avro
from avro import schema, datafile, io
import csv

URL = "https://app.nomination.fr/login"
login = "adouillard@op-rate.com"
password = "wef3YGR1mep.cqz1yrz"
target_url = "https://app.nomination.fr/my-lists/list/33109-530815"
base_url = "https://app.nomination.fr"

start_time = time.time()

# Open a browser window with Selenium
driver = webdriver.Chrome()
driver.maximize_window()

# Perform the login using Selenium
driver.get(URL)
driver.find_element(By.CSS_SELECTOR, "input#username").send_keys(login)
driver.find_element(By.CSS_SELECTOR, "input#password").send_keys(password)
driver.find_element(By.CSS_SELECTOR, "button[type='submit']").click()
time.sleep(3)

# go to list
driver.get(target_url)
time.sleep(2)

# Get the page source and create a BeautifulSoup object
html = driver.page_source
soup = bs(html, 'html.parser')

# Define the Avro schema for Contact
contact_schema = schema.parse('''
    {
        "type": "record",
        "name": "Contact",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "job", "type": "string"},
            {"name": "company", "type": "string"},
            {"name": "zip", "type": ["null", "string"], "default": null},
            {"name": "phone", "type": ["null", "string"], "default": null},
            {"name": "email", "type": ["null", "string"], "default": null}
        ]
    }
''')

# put all the content of href_values.csv file in my_rows
my_rows = []
with open('href_values_og.csv', 'r') as file:
    reader = csv.reader(file)
    first_line = next(reader)
    for row in reader:
        my_rows.append(row)

#put all the content of href_values_scrapped.csv file in scrapped
scrapped_before = []
with open('href_values_scrapped.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader)
    for row in reader:
        scrapped_before.append(row)

scrapped_now = []


nb_of_contacts = len(my_rows)

index_of_row_to_delete=[]
delete_after_scrap=[]

with open("contacts.avro", "ab") as avro_file:
    writer = datafile.DataFileWriter(avro_file, io.DatumWriter(), contact_schema)

    #for i in range(nb_of_contacts):
    for i in range(50):
        url = my_rows[i][0]
        id = my_rows [i][1]

        for row in scrapped_before :
            if row[1]==id :
                index_of_row_to_delete.append(i+1)
                i += 1  # Increment i
                break #exit the loop and go to next row in my_rows unscrapped   

        driver.get(url)
        time.sleep(3)
        html = driver.page_source
        soup = bs(html, 'html.parser')

        name_OH = soup.find('h1')
        name = name_OH.get_text(strip=True)

        job_OH = soup.find('p', class_='contact-profile__description__position')
        job = job_OH.get_text(strip=True)

        company_OH = soup.find('a', class_='contact-profile__organisation__link')
        company = company_OH.get_text(strip=True)

        zip_OH = soup.select_one('.conditional-address > span:nth-child(1) > span:nth-child(3)')

        if zip_OH:
            zip = zip_OH.text.strip()
        else:
            zip = "-"

        phone_element = soup.find('a', href=lambda href: href and href.startswith('tel:'))

        if phone_element:
            phone = phone_element.get_text(strip=True)
        else:
            phone = "-"

        email_element = soup.find('a', href=lambda href: href and href.startswith('mailto:'))

        if email_element:
            email = email_element.get_text(strip=True)
        else:
            email = "-"

        contact_data = {"name": name, "job": job, "company": company, "zip": zip, "phone": phone, "email": email}
        writer.append(contact_data)
        print(f"Contact {i+1} successfully loaded")

        scrapped_now.append(my_rows[i])
        delete_after_scrap.append(i)

    writer.close()

print(index_of_row_to_delete)
# here open href_values_scrapped and put my_rows_scrapped inside

with open('href_values_scrapped.csv', 'a', newline='') as file:
    writer = csv.writer(file)
    for row in scrapped_now:
        writer.writerow(row)
  
# here open href_values_og and delete index_of_row_to_delete from it

# Create a new list to store the filtered rows
filtered_rows = [row for i, row in enumerate(my_rows) if i not in delete_after_scrap]

# Write the filtered rows to a new file
with open('href_values_og.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(first_line)  # Write the first line
    writer.writerows(filtered_rows)  # Write the new rows

# Close the browser window
driver.quit()

end_time = time.time()
execution_time = end_time - start_time

print(f"Execution time: {execution_time} seconds")
