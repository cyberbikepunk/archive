""" This script transforms an ugly csv file into a pretty pdf table. """

from pandas import read_csv
from weasyprint import HTML
from jinja2 import Environment, FileSystemLoader

data_csv = '/Users/loicjounot/Documents/data_november_bobby.csv'

data = read_csv(data_csv)
data.info()

# Generate some global statistics, like the total invoice
monthly_total_invoiced = data['total'].sum()
print('Total invoiced =', monthly_total_invoiced, '€')

environment = Environment(loader=FileSystemLoader('.'))
report_template = environment.get_template("template.html")

template_variables = {'total': monthly_total_invoiced, 'billing_breakdown_table': data.to_html()}
html_out = report_template.render(template_variables)


HTML(string=html_out).write_pdf('/Users/loicjounot/Documents/data_november_bobby.pdf')