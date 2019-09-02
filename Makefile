run:
	gradle run
build:
	gradle build
clean:
	gradle clean
truncatedb:
	gradle flywayClean
	gradle flywayMigrate
accessdb:
	mysql -u points_driver --database=Points --password=points_password
