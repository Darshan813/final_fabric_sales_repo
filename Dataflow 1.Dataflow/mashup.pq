[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "bronze Customers_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "Customer_ID", DestinationColumnName = "Customer_ID"], [SourceColumnName = "Gender", DestinationColumnName = "Gender"], [SourceColumnName = "Age", DestinationColumnName = "Age"], [SourceColumnName = "Spending_Score", DestinationColumnName = "Spending_Score"], [SourceColumnName = "Age Group", DestinationColumnName = "Age Group"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared #"bronze Customers" = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "ff9fc356-755f-447d-8c6b-ccbd95cc0f83"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "733a6238-5d7a-492a-ac3b-9d45da3ca600"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "bronze.Customers", ItemKind = "Table"]}[Data],
  #"Renamed columns" = Table.RenameColumns(#"Navigation 2", {{"Genre", "Gender"}, {"CustomerID", "Customer_ID"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"Customer_ID", Int64.Type}, {"Age", Int64.Type}, {"Annual_Income_(k$)", type number}, {"Spending_Score", Int64.Type}}),
  #"Inserted conditional column" = Table.AddColumn(#"Changed column type", "Age Group", each if [Age] < 30 then "Young Adult" else if [Age] < 50 then "Adult" else "Senior"),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Inserted conditional column", {{"Age Group", type text}})
in
  #"Changed column type 1";
shared #"bronze Customers_DataDestination" = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "ff9fc356-755f-447d-8c6b-ccbd95cc0f83"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "733a6238-5d7a-492a-ac3b-9d45da3ca600"]}[Data],
  TableNavigation = Navigation_2{[Id = "silver.Customers", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
