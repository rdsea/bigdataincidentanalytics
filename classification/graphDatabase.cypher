CREATE (Incident:Element{name:'Incident'})
CREATE (DataAsset:Element{name:'Data asset'})
CREATE (InMotion:DataAsset{name:'In motion'})
CREATE (InProcessing:DataAsset{name:'In processing'})
CREATE (AtRest:DataAsset{name:'At rest'})
CREATE (DataPipeline:Element{name:'Data Pipeline'})
CREATE (Phase:Element{name:'Analytics phase'})
CREATE (Collection:Phase{name:'Collection'})
CREATE (Preparation:Phase{name:'Preparation'})
CREATE (Analysis:Phase{name:'Analysis'})
CREATE (Delivery:Phase{name:'Delivery'})
CREATE (SetOfEvents:Element{name:'Set of events'})
CREATE (Monitoring:Element{name:'Monitoring'})
CREATE (PointInTime:Element{name:'Point in time'})
CREATE (Functionality:Element{name:'Functionality'})
CREATE (FunctionalArea:Element{name:'Functional area'})
CREATE (DataSource:FunctionalArea{name:'Data Source'})
CREATE (JobSchedulingSystem:FunctionalArea{name:'Job Scheduling System'})
CREATE (DataStorage:FunctionalArea{name:'Data Storage'})
CREATE (DataExtractionIngestion:FunctionalArea{name:'Data Extraction Ingestion'})
CREATE (DataLoadingPreprocessing:FunctionalArea{name:'Data Loading Preprocessing'})
CREATE (DataProcessing:FunctionalArea{name:'Data Processing'})
CREATE (DataAnalysis:FunctionalArea{name:'Data Analysis'})
CREATE (DataLoadingTransformation:FunctionalArea{name:'Data Loading Transformation'})
CREATE (InterfacingVisualization:FunctionalArea{name:'Interfacing Visualization'})
CREATE (Software:Element{name:'Software'})
CREATE (ApplicationSoftware:Software{name:'Application Software'})
CREATE (Platform:Software{name:'Platform'})
CREATE (SystemSoftware:Element{name:'System Software'})
CREATE (Infrastructure:Element{name:'Infrastructure'})
CREATE (Stakeholder:Element{name:'Stakeholder'})
CREATE (DataProvider:Stakeholder{name:'Data provider'})
CREATE (DataEngineer:Stakeholder{name:'Data engineer'})
CREATE (ApplicationProvider:Stakeholder{name:'Application provider'})
CREATE (ServiceProvider:Stakeholder{name:'Service provider'})
CREATE (DataConsumer:Stakeholder{name:'Data consumer'})
CREATE (Effect:Element{name:'Effect'})
CREATE (UnplannedInterruption:Effect{name:'Unplanned Interruption'})
CREATE (ReductionOfQuality:Effect{name:'Reduction of Quality'})
CREATE (FailureWithoutImpact:Effect{name:'Failure without impact'})
CREATE (Cause:Element{name:'Cause'})
CREATE (Technological:Cause{name:'Technological'})
CREATE (HumanAction:Cause{name:'Human Action'})
CREATE (NaturalPhenomenon:Cause{name:'Natural Phenomenon'})
CREATE (SLA:Element{name:'SLA'})
CREATE (QualityOfAnalytics:SLA{name:'Quality of analytics'})
CREATE (DataQuality:QualityOfAnalytics{name:'Data quality'})
CREATE (AnalyticsTime:QualityOfAnalytics{name:'Analytics time'})
CREATE (QualityOfService:SLA{name:'Quality of service'})
CREATE (Cost:QualityOfService{name:'Cost'})
CREATE (Performance:QualityOfService{name:'Performance'})
CREATE (ServiceLevelObjective:Element{name:'Service level objective'})
CREATE (ServiceLevelIndicator:Element{name:'Service level indicator'})
//Here is the definition of the example 
CREATE (ApacheNifi:DataPipeline{name:'ApacheNifi'})
CREATE (MQTT:DataPipeline{name:'MQTT'})
CREATE (HDFS:DataPipeline{name:'HDFS'})
CREATE (Linux:SystemSoftware{name:'Linux'})
CREATE (Archibald:Infrastructure{name:'Archibald'})
//CREATE (FrameworkProvider:Stakeholder{name:'Framework Provider'})
//Here the corresponding provider are added
CREATE (Provider1:ServiceProvider{name:'Streaming Inc.'})
CREATE (Provider2:ServiceProvider{name:'FrameworkProvider2'})
CREATE (Provider3:DataProvider{name:'DataProvider1'})
CREATE
  (Incident)-[:HAPPENSAT]->(PointInTime),
  (Incident)-[:DETECTEDFROM]->(SetOfEvents),
  (Incident)-[:HAS]->(Effect),
  (Incident)-[:HAS]->(Cause),
  (Incident)-[:EFFECTS]->(Stakeholder),
  (Effect)-[:IS]->(UnplannedInterruption),
  (Effect)-[:IS]->(ReductionOfQuality),
  (Effect)-[:IS]->(FailureWithoutImpact),
  (Cause)-[:IS]->(Technological),
  (Cause)-[:IS]->(HumanAction),
  (Cause)-[:IS]->(NaturalPhenomenon),
  (Stakeholder)-[:RESPONSIBLE]->(Incident),
  (Stakeholder)-[:IS]->(DataProvider),
  (Stakeholder)-[:IS]->(DataEngineer),
  (Stakeholder)-[:IS]->(ApplicationProvider),
  (Stakeholder)-[:IS]->(ServiceProvider),
  (Stakeholder)-[:IS]->(DataConsumer),
  (SetOfEvents)<-[:STEMSFROM]-(Monitoring),
  (SetOfEvents)<-[:HAPPENSAT]-(PointInTime),
  (DataPipeline)-[:MONITORED]->(Monitoring),
  (DataAsset)-[:HASSTATE]->(InMotion),
  (DataAsset)-[:HASSTATE]->(AtRest),
  (DataAsset)-[:HASSTATE]->(InProcessing),
  (DataAsset)-[:RESIDES]->(DataPipeline),
  (DataPipeline)-[:BELONGSTO]->(Phase),
  (DataPipeline)-[:sup]->(DataPipeline),
  (DataPipeline)-[:REALISEDWITH]->(Functionality),
  (Software)-[:IS]->(ApplicationSoftware),
  (Software)-[:IS]->(Platform),
  (Phase)-[:IS]->(Collection),
  (Phase)-[:IS]->(Preparation),
  (Phase)-[:IS]->(Analysis),
  (Phase)-[:IS]->(Delivery),
  (Functionality)-[:BELONGSTO]->(FunctionalArea),
  (Functionality)-[:BELONGSTO]->(Software),
  (Functionality)-[:MONITORED]->(Monitoring),
  (FunctionalArea)-[:IS]->(DataSource),
  (FunctionalArea)-[:IS]->(JobSchedulingSystem),
  (FunctionalArea)-[:IS]->(DataStorage),
  (FunctionalArea)-[:IS]->(DataExtractionIngestion),
  (FunctionalArea)-[:IS]->(DataLoadingPreprocessing),
  (FunctionalArea)-[:IS]->(DataProcessing),
  (FunctionalArea)-[:IS]->(DataAnalysis),
  (FunctionalArea)-[:IS]->(DataLoadingTransformation),
  (FunctionalArea)-[:IS]->(InterfacingVisualization),
  (Software)-[:HOSTEDON]->(SystemSoftware),
  (SystemSoftware)-[:HOSTEDON]->(Infrastructure),
  (Software)-[:OWNED]->(Stakeholder),
  (Software)-[:MONITORED]->(Monitoring),
  (Infrastructure)-[:MONITORED]->(Monitoring),
  (Infrastructure)-[:OWNED]->(Stakeholder),
  (ServiceLevelIndicator)-[:MONITORED]->(Monitoring),
  (ServiceLevelIndicator)-[:BASISOF]->(ServiceLevelObjective),
  (ServiceLevelObjective)-[:BASISOF]->(SLA),
  (SLA)-[:IS]->(QualityOfAnalytics),
  (SLA)-[:IS]->(QualityOfService),
  (QualityOfAnalytics)-[:IS]->(DataQuality),
  (QualityOfAnalytics)-[:IS]->(AnalyticsTime),
  (QualityOfService)-[:IS]->(Performance),
  (QualityOfService)-[:IS]->(Cost),
  (Stakeholder)-[:ESTABLISHES]->(SLA),
//Again here starts the prototype
  (ApacheNifi)-[:IS]->(DataPipeline),
  (MQTT)-[:IS]->(DataPipeline),
  (HDFS)-[:IS]->(DataPipeline),
  (MQTT)-[:PREDECESSOR]->(ApacheNifi),
  (ApacheNifi)-[:PREDECESSOR]->(HDFS),
  (ApacheNifi)-[:BELONGSTO]->(DataLoadingPreprocessing),
  (ApacheNifi)-[:IS]->(ApplicationSoftware),
  (ApacheNifi)-[:BELONGSTO]->(Preparation),
  (ApacheNifi)-[:ASSETSTATE]->(InProcessing),
  (ApacheNifi)-[:HOSTEDON]->(Linux),
  (Linux)-[:IS]->(SystemSoftware),
  (Stakeholder)-[:IS]->(ServiceProvider),
  (Provider1)-[:OWNS]->(ApacheNifi),
  (Provider2)-[:OWNS]->(HDFS),
  (Provider3)-[:OWNS]->(MQTT),
  (Provider1)-[:IS]->(ServiceProvider),
  (Provider2)-[:IS]->(ServiceProvider),
  (Provider3)-[:IS]->(DataProvider)
  ;
