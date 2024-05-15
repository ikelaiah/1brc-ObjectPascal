unit WeatherStation;

{$mode objfpc}{$H+}{$J-}{$modeSwitch advancedRecords}

interface

uses
  Classes
  , SysUtils
  , streamex
  , bufstream
  //, lgHashMap
  , generics.Collections
  , mormot.core.buffers
  , mormot.core.os
  , mormot.core.collections

  {$IFDEF DEBUG}
  , Stopwatch
  {$ENDIF}
  , WeatherStation.Common;

type
  // Create a class to encapsulate the temperature observations of each weather station.
  TWeatherStation = class
  private
    fname: string;
    weatherDictionary: TWeatherDictionary;
    weatherStationList: TStringList;
    lookupStrFloatToIntList: TValidTemperatureDictionary;
    procedure CreateLookupTemp;
    procedure ReadMeasurements;
    procedure ReadMeasurementsBuffered;
    procedure ReadMeasurementsMMT;
    procedure ProcessChunk(const buffer: TBytes; lineBreakPos: int64);
    procedure ParseStationAndTemp(const line: string);
    procedure AddCityTemperatureLG(const cityName: string; const newTemp: int64);
    procedure SortWeatherStationAndStats;
    procedure PrintSortedWeatherStationAndStats;
  public
    constructor Create(const filename: string);
    destructor Destroy; override;
    // The main algorithm to process the temp measurements from various weather stations
    procedure ProcessMeasurements;
  end;

implementation

constructor TWeatherStation.Create(const filename: string);
begin
  // Assign filename
  fname := filename;

  // Create a lookup
  self.lookupStrFloatToIntList :=
    Collections.specialize NewKeyValue<string, int64>([kvoThreadSafe, kvoKeyCaseInsensitive]);

  // Set expected capacity - saves 10 seconds.
  self.lookupStrFloatToIntList.Capacity := 2048;

  // Create a dictionary
  self.weatherDictionary := Collections.specialize NewKeyValue<string,
    PStat>([kvoThreadSafe, kvoKeyCaseInsensitive]);
  self.weatherDictionary.Capacity := 44691;

  // Create a TStringList for sorting
  weatherStationList := TStringList.Create;
end;

destructor TWeatherStation.Destroy;
var
  index: int64;
begin

  // Free the lookup dictionary
  // self.lookupStrFloatToIntList.Free;


  // Free the dictionary - 1. Free PStat first
  for index := 0 to self.weatherDictionary.Count - 1 do
    Dispose(PStat(self.weatherDictionary.GetValue(index)));

  // Free the dictionary - 2. Finally free the container itself
  // weatherDictionary.Free;

  // Free TStringList dictionary
  weatherStationList.Free;

end;

procedure TWeatherStation.CreateLookupTemp;
var
  startTemp: int64 = -1000;
  finishTemp: int64 = 1000;
  currentTemp: int64;
begin

  currentTemp := startTemp;

  while currentTemp <= finishTemp do
  begin
    self.lookupStrFloatToIntList.Add(formatfloat('0.0', currentTemp / 10), currentTemp);
    currentTemp := currentTemp + 1;
  end;

  {$ifdef DEBUG}
   for pair in self.lookupStrFloatToIntList do
    WriteLn('We have key: ', pair.Key, ' with value of: ', pair.Value);

   Writeln(self.lookupStrFloatToIntList.Count);
  {$endif DEBUG}

end;

procedure TWeatherStation.PrintSortedWeatherStationAndStats;
var
  outputList: string;
  index: int64;
begin

  {$IFDEF DEBUG}
  // Display the line.
  WriteLn('Printing now: ', DateTimeToStr(Now));
  {$ENDIF DEBUG}

  if self.weatherStationList.Count = 0 then
  begin
    WriteLn('Nothing to print. The list is empty.');
    Exit;
  end;

  outputList := '';
  // Print the weather station and the temp stat
  for index := 0 to self.weatherStationList.Count - 1 do
    outputList := outputList + self.weatherStationList[index];

  // Remove last comma and space; ', ', a neat trick from Gus.
  SetLength(outputList, Length(outputList) - 2);
  WriteLn('{', outputList, '}');

  {$IFDEF DEBUG}
  // Display the line.
  WriteLn('Printing done: ', DateTimeToStr(Now));
  {$ENDIF DEBUG}
end;

procedure TWeatherStation.SortWeatherStationAndStats;
var
  pair: TStationTempPair;
begin

  {$IFDEF DEBUG}
  // Display the line.
  WriteLn('Sorting now: ', DateTimeToStr(Now));
  {$ENDIF DEBUG}

  if self.weatherDictionary.Count = 0 then
  begin
    WriteLn('Nothing to Sort.');
    Exit;
  end;

  for pair in self.weatherDictionary do
  begin
    // Don't worry about the ', ' at the end. The final printout will take care of it.
    self.weatherStationList.Add(pair.Key + '=' + pair.Value^.ToString + ', ');
  end;

  self.weatherStationList.CustomSort(@CustomTStringListComparer);

  {$IFDEF DEBUG}
  // Display the line.
  WriteLn('Sorting done: ', DateTimeToStr(Now));
  {$ENDIF DEBUG}
end;

procedure TWeatherStation.AddCityTemperatureLG(const cityName: string;
  const newTemp: int64);
var
  stat: PStat;
begin
  // If city name esxists, modify temp as needed
  if self.weatherDictionary.TryGetValue(cityName, stat) then
  begin

    // Update min and max temps if needed
    // Re-arranged the if statement, to achieve minimal if checks.
    // This saves approx 15 seconds when processing 1 billion row.
    if (newTemp < stat^.min) or (newTemp > stat^.max) then
    begin
      // If the temp lower then min, set the new min.
      if newTemp < stat^.min then
        stat^.min := newTemp;
      // If the temp higher than max, set the new max.
      if newTemp > stat^.max then
        stat^.max := newTemp;
    end;
    // Add count for this city.
    stat^.sum := stat^.sum + newTemp;

    // Increase the counter
    stat^.cnt := stat^.cnt + 1;


    // Update the stat of this city
    // self.weatherDictionary.AddOrSetValue(cityName, stat);
    {$IFDEF DEBUG}
    // Display the line.
    WriteLn('Updated: ', cityName);
    {$ENDIF DEBUG}
  end
  else
  begin
    // Re-arranged this if portion also to achieve minimal if checks.
    // This saves approx 15 seconds when processing 1 billion row.
    // If city name doesn't exist add a new entry
    New(stat);
    stat^.min := newTemp;
    stat^.max := newTemp;
    stat^.sum := newTemp;
    stat^.cnt := 1;
    self.weatherDictionary.Add(cityName, stat);

    {$IFDEF DEBUG}
    // Display the line.
    WriteLn('weatherDictionary count: ', inttostr(self.weatherDictionary.Count));
    WriteLn('Added: ', cityName);
    {$ENDIF DEBUG}
  end;
end;

procedure TWeatherStation.ParseStationAndTemp(const line: string);
var
  delimiterPos: integer;
  strFloatTemp: shortstring;
  parsedTemp: int64 = 0;
  pair: TLookupPair;
begin

  if length(line) = 0 then Exit;

  // Get position of the delimiter
  delimiterPos := PosFast(';', line);

  {$IFDEF DEBUG}
  WriteLn('Received: ', line);
  WriteLn('Delimiter Pos: ', delimiterPos);
  {$ENDIF DEBUG}

  if delimiterPos > 0 then
  begin

    // Get the weather station name
    // Using Copy and POS instead of SplitString - as suggested by Gemini AI.
    // This part saves 3 mins faster when processing 1 billion rows.

    // No need to create a string
    // parsedStation := Copy(line, 1, delimiterPos - 1);

    strFloatTemp := Copy(line, delimiterPos + 1, Length(line));

    // Using a lookup value speeds up 30-45 seconds
    if self.lookupStrFloatToIntList.TryGetValue(strFloatTemp, parsedTemp) then
    begin
      self.AddCityTemperatureLG(Copy(line, 1, delimiterPos - 1), parsedTemp);
    end;
  end;
end;

{This approach turned out to be the faster method than the TCSVDocument method.}
procedure TWeatherStation.ReadMeasurements;
var
  fileStream: TFileStream;
  streamReader: TStreamReader;
begin

  // Open the file for reading
  fileStream := TFileStream.Create(self.fname, fmOpenRead);
  try
    streamReader := TStreamReader.Create(fileStream, 65536 * 2, False);
    try
      // Read and parse chunks of data until EOF -------------------------------
      while not streamReader.EOF do
      begin
        self.ParseStationAndTemp(streamReader.ReadLine);
        self.ParseStationAndTemp(streamReader.ReadLine);
      end;// End of read and parse chunks of data ------------------------------
    finally
      streamReader.Free;
    end;
  finally
    // Close the file
    fileStream.Free;
  end;
end;


procedure TWeatherStation.ProcessChunk(const buffer: TBytes; lineBreakPos: int64);
var
  strStream: TStringStream;
  streamReader: TStreamReader;
  chunkBytes: TBytes;
begin

  // Check if lineBreakPos is within bounds
  if (lineBreakPos > 0) and (lineBreakPos <= Length(buffer)) then
  begin
    // Copy the portion of buffer from 0 to lineBreakPos into chunkBytes
    SetLength(chunkBytes, lineBreakPos);
    Move(buffer[0], chunkBytes[0], lineBreakPos);

    // Create a TStringStream with the chunkBytes
    strStream := TStringStream.Create(chunkBytes);
    try
      streamReader := TStreamReader.Create(strStream);
      try
        while not streamReader.EndOfStream do
        begin
          self.ParseStationAndTemp(streamReader.ReadLine);
        end;

      finally
        streamReader.Free;
      end;

    finally
      strStream.Free;
    end;
  end
  else
  begin
    WriteLn('Invalid lineBreakPos:', lineBreakPos);
  end;
end;


procedure TWeatherStation.ReadMeasurementsBuffered;
const
  defaultChunkSize = 8388608;
var
  fileStream: TBufferedFileStream;
  buffer: TBytes;
  bytesRead, totalBytesRead, chunkSize, lineBreakPos, chunkIndex: int64;
begin

  chunkSize := defaultChunkSize * 1;

  // Open the file for reading
  fileStream := TBufferedFileStream.Create(self.fname, fmOpenRead);
  try
    // Allocate memory buffer for reading chunks
    SetLength(buffer, chunkSize);
    try
      totalBytesRead := 0;
      chunkIndex := 0;

      // Read and parse chunks of data until EOF
      while totalBytesRead < fileStream.Size do
      begin
        bytesRead := fileStream.Read(buffer[0], chunkSize);
        Inc(totalBytesRead, bytesRead);

        // Find the position of the last newline character in the chunk
        lineBreakPos := BytesRead;
        while (lineBreakPos > 0) and (Buffer[lineBreakPos - 1] <> Ord(#10)) do
          Dec(lineBreakPos);

        { Now, must ensure that if the last byte read in the current chunk
          is not a newline character, the file pointer is moved back to include
          that byte and any preceding bytes of the partial line in the next
          chunk's read operation.

          Also, no need to update the BytesRead variable in this context because
          it represents the actual number of bytes read from the file, including
          any partial line that may have been included due to moving the file
          pointer back.
          Ref: https://www.freepascal.org/docs-html/rtl/classes/tstream.seek.html}
        if lineBreakPos < bytesRead then
          fileStream.Seek(-(bytesRead - lineBreakPos), soCurrent);

        // Process the chunk
        self.ProcessChunk(buffer, lineBreakPos);

        // Display user feedback
        WriteLn('Chunk ', chunkIndex, ', Total bytes read:', IntToStr(totalBytesRead));

        // Increase chunk index - a counter
        Inc(chunkIndex);
      end;
    finally
      // No need to free TBytes.
    end;
  finally
    // Close the file
    fileStream.Free;
  end;
end;


procedure TWeatherStation.ReadMeasurementsMMT;
var
  mmt: TMemoryMapText;
  index, lineCount: int64;
begin

  mmt := TMemoryMapText.Create(self.fname);
  try

    lineCount := mmt.Count;

    for index := 0 to lineCount - 1 do
    begin
      self.ParseStationAndTemp(mmt.Lines[index]);
    end;

  finally
    mmt.Free;
  end;

end;


// The main algorithm
procedure TWeatherStation.ProcessMeasurements;
begin
  self.CreateLookupTemp;
  self.ReadMeasurements;
  //self.ReadMeasurementsBuffered;
  //self.ReadMeasurementsMMT;
  self.SortWeatherStationAndStats;
  self.PrintSortedWeatherStationAndStats;
end;

end.
