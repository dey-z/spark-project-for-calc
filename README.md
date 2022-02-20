# spark-project-for-calc
>practice for dataframes, matrix, rdds etc in spark  
>has many unrequired dependencies to be used maybe in future/to be deleted if not used

## (1)project dependencies
- sbt: 1.5.7(no install required)
- scala: 2.11.12(no install required)
- spark: 2.4.4(no install required)
- scalapb: 0.10.0(no install required)
- java: 1.8(install required in dev environment)

## (2)editor
- Editor: IntelliJ

## (3)scalafmt settings
### using scalafmt settings in root of the project
https://www.jetbrains.com/help/idea/work-with-scala-formatter.html#scalafmt_config

## (4)copy application.local.conf.sample to application.local.conf(if needed)
###  application.local.conf should have necessary settings template
- cp src/main/resources/application.local.conf.sample src/main/resources/application.local.conf

## (5)Check output
>make a "run" sbt task in Run/Debug Configurations  
