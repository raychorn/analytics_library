import export_report
import amqp_consumer_sub

print "table_columns:"
col_list = export_report.table_columns("InstallationInfo")
print col_list

print
print "group_columns:"
col_dict = export_report.group_columns("sprintcm")
print col_dict

print
prefix = amqp_consumer_sub.prefix_columns("1,2,17,18,uuid")
print
print prefix
