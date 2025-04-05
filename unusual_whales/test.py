import requests

session = requests.Session()

usr_name = 'sonurenigunta@gmail.com'
passwrd = 'P!F5h3tn9PIiB325'

posturl = 'https://phx.unusualwhales.com/api/users/login'

payload =   {"email":"sonurenigunta@gmail.com","password":"P!F5h3tn9PIiB325","g-recaptcha-response":"03AFcWeA5ACN5S56HcuV4SxiNM2v7ibtzxzhpc0TNi5FS2nnAnVNJ5JVIn_fhfGF-rpUwsxVqKubpawROkMWOZxwbR1U2hwtOzNMsiRUWlq3oz1-aPyfUBu6rcf0IoLaecg864nHNT6j5k7oDloOCh2Ptx5j2i4V_TfjT1fTCxyw8n6airkAGkqgaFjnhDIrHjWrwI1XX4ty8Wisyrsx-9_it4f-q1nnMw3z5GgLi812CSy_aQbuIxtrE0yBQ_eF52V_ZkFwOqwK1dYyF7TWf9qDFMbxwwnh68vVL5DXsTJ0twnN2vu5aGDrDBFxvGmLL5f_RBxyncd9anV7mw1U1f1BET0xTqolN2abEflzRrsV8us7sdoUO-q8aTJHxLAgU5cb6jg8AAfxWuilp4C4Fq_lZq_jnv1JRVB_-udnzg_PbcvsZoJj0qeqZv6HgtcEnKO91pjLBruIQ5NQm9K9j7yBnYHkLKA1saibJv0E2ByIfXVpQxPa4XaMWVi9Cus-ICR0NOcmRsyirg9tNHm56fk-Gg9UDjWAbEvQn0sXdmQZzhLINDYFZYPPd4wctByhUS8nhzM9grDIikb8AVQCsFt6NtOiutL_SS4u8l253SwqO4tCpB5O3Wb7WOVX5H-T8DNUkQkfJYOAlsbYZGciRAIWmLCO2Oc8OTIUTlmdOfZ8R4dNppP5yXnKxEweQAP-_oKcKLpLw8-aSFoZQhY2aKub0-E9NDpH4t6RjKhPwrk6_uZ_ajhASrH1CGe0o0lfATfukLjLARFyBQrQGteW-SA0vusIWpTFwo1ZoBSf0wdeLl-4JOU41giBi20ensLZC9Be0p5Zv8Qu_0A_QgtQO8iKbVM1c_XmrT7CLLKYyk9_JZMTZ4IdK74XO60qwsD2KO4K1j-urt1LAZ2BVFLoWRGgv4jbkMtfLuC1SEWZ7IZpx9sY6phanioOee9N7frXMVwcyz4c9YStwVWANJgA_6iCjIfNtrBABN3IleskYL7CPxyhUEREX3fnw","mfa_code":"false"}

headers = {
    "Host": "phx.unusualwhales.com",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Referer": "https://unusualwhales.com/",
    "authorization": "Bearer null",
    "content-type": "application/json",
    "uw-path": "/login",
    "Content-Length": "1125",
    "DNT": "1",
    "Sec-GPC": "1",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "Priority": "u=4",
    "Pragma": "no-cache",
    "Cache-Control": "no-cache",
    "TE": "trailers"
}

response = session.post(posturl, headers=headers, json=payload)
print(response.status_code)