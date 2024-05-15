unit WeatherStation.Common;

{$mode objfpc}{$H+}{$J-}{$modeSwitch advancedRecords}


interface

uses
  Classes
  , SysUtils
  , generics.Collections
  , Baseline.Common
  , mormot.core.collections;

type
  { Create a record of temperature stats.

    Borrowed the concept from go's approach to improve performance, save floats as int64.
    This saved ~2 mins processing time for processing 1 billion rows.}
  TStat = record
  var
    min: int64;
    max: int64;
    sum: int64;
    cnt: int64;
  public
    function ToString: string;
  end;

  {Using pointer to TStat saves approx. 30-60 seconds for processing 1 billion rows}
  PStat = ^TStat;


type
  // Using this dictionary, now approx 4 mins faster than Generics.Collections.TDictionary
  // THashMap<shortstring, PStat> - takes around 120s.
  // TFastHash<shortstring, PStat> - takes around 100s.
  TWeatherDictionary = specialize IKeyValue<string, PStat>;

  TStationTempPair = specialize TPair<string, PStat>;

type
  // a type for storing valid lookup temperature
  TValidTemperatureDictionary = specialize IKeyValue<string, int64>;

  TLookupPair = specialize TPair<string, int64>;

  {
    A custom comparer for TStringList.

    The following procedure Written by Székely Balázs for the 1BRC for Object Pascal.
    URL: https://github.com/gcarreno/1brc-ObjectPascal/tree/main
  }
function CustomTStringListComparer(AList: TStringList;
  AIndex1, AIndex2: integer): integer;

function PosFast(const delimiter: char; const line: string): int64;

// Remove dots from a string from the right
function RemoveDots(const line: string): string;


implementation

function CustomTStringListComparer(AList: TStringList;
  AIndex1, AIndex2: integer): integer;
var
  Pos1, Pos2: integer;
  Str1, Str2: string;
begin
  Result := 0;
  Str1 := AList.Strings[AIndex1];
  Str2 := AList.Strings[AIndex2];
  Pos1 := Pos('=', Str1);
  Pos2 := Pos('=', Str2);
  if (Pos1 > 0) and (Pos2 > 0) then
  begin
    Str1 := Copy(Str1, 1, Pos1 - 1);
    Str2 := Copy(Str2, 1, Pos2 - 1);
    Result := CompareStr(Str1, Str2);
  end;
end;


// Find the position of ; count down to
function PosFast(const delimiter: char; const line: string): int64;
var
  index: int64;
begin
  Result := length(line);
  for index := Length(line) downto 1 do
  begin
    if line[index] = delimiter then
    begin
      Result := index;
      Exit;
    end;
  end;
end;

// Remove dots from a string from the right
function RemoveDots(const line: string): string;
var
  index: int64;
begin
  Result := line;
  for index := Length(Result) downto 1 do
  begin
    if Result[index] = '.' then
      Delete(Result, index, 1);
  end;
end;

function TStat.ToString: string;
var
  minR, meanR, maxR: double; // Store the rounded values prior saving to TStringList.
begin
  minR := RoundExDouble(self.min / 10);
  maxR := RoundExDouble(self.max / 10);
  meanR := RoundExDouble(self.sum / self.cnt / 10);
  Result := FormatFloat('0.0', minR) + '/' + FormatFloat('0.0', meanR) +
    '/' + FormatFloat('0.0', maxR);
end;


end.
