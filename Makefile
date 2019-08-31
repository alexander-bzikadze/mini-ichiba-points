run:
	gradle run
build:
	gradle build
clean:
	gradle clean
truncatedb:
	mysql -u points_driver --database=Points --password=points_password <src/sql/create_tables.sql
accessdb:
	mysql -u points_driver --database=Points --password=points_password
