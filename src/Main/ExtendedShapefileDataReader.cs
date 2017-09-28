using Microsoft.SqlServer.Types;
using Reimers.Esri;
using SQLSpatialTools;
using System;
using System.Data;
using System.Data.Odbc;
using System.IO;
using USC.GISResearchLab.Common.Core.KML;
using USC.GISResearchLab.Common.Core.TextEncodings.Soundex;
using USC.GISResearchLab.Common.Utils.Numbers;

namespace USC.GISResearchLab.Common.Shapefiles.ShapefileReaders
{
    /// <summary>
    /// Holds the data contained in an ESRI shapefile.
    /// </summary>
    public class ExtendedShapefileDataReader : ShapefileDataReader
    {

        #region Events


        /// <summary>
        /// Triggered when an unknown shape is encountered in a shapfile definition.
        /// </summary>
        /// <remarks>
        /// <para>Tie an event handler to this event to handle reading of the unknown record.</para>
        /// </remarks>
        public new event ReadUnkownShape UnknownRecord;

        /// <summary>
        /// Triggered when a point has been parsed in the shapefile definition.
        /// </summary>
        /// <remarks>
        /// <para>Tie an event handler to this event to handle custom conversions from coordinate types other than WGS84. The arguments are the raw numbers read in the shapefile.</para>
        /// </remarks>
        public new event PointParsedHandler PointParsed;

        /// <summary>
        /// Triggered when a record has been read from the stream.
        /// </summary>
        /// <remarks>
        /// <para>Tie an event handler to this event to be notified when a record has been read from the stream.</para>
        /// </remarks>
        public new event PercentReadHandler PercentRead;


        /// <summary>
        /// Triggered when a record has been read from the stream.
        /// </summary>
        /// <remarks>
        /// <para>Tie an event handler to this event to be notified when a record has been read from the stream.</para>
        /// </remarks>
        public new event RecordsReadHandler RecordsRead;

        #endregion

        #region Properties

        public bool IncludeSoundex { get; set; }
        public string[] SoundexColumns { get; set; }
        public bool IncludeSoundexDM { get; set; }
        public string[] SoundexDMColumns { get; set; }
        public bool IncludeLineEndPoints { get; set; }
        public string[] IncludeLineEndPointsColumns { get; set; }
        public bool ShouldSplitAddressRanges { get; set; }
        public string[] SplitAddressRangesColumns { get; set; }
        public bool ShouldAddEvenOddFlag { get; set; }
        public string[] AddEvenOddFlagColumns { get; set; }
        public bool ShouldIncludeGeometryProjected { get; set; }
        public bool ShouldIncludeArea { get; set; }
        public bool ShouldIncludeCentroid { get; set; }
        public bool ShouldTrimStringData { get; set; }

        #endregion

        #region Constructor

        public ExtendedShapefileDataReader()
        {
        }


        public ExtendedShapefileDataReader(string FileName) : base(FileName) { }

        #endregion

        public override bool NextFeature()
        {
            bool ret = true;
            try
            {

                CurrentShapefileRecord = null;

                try
                {
                    if ((ShapeStream == null) || (FileDataReader == null))
                    {
                        ResetNextFeature();
                    }
                }
                catch (Exception e1)
                {
                    if (ShouldAbortOnError)
                    {
                        throw new Exception("Error reseting next feature: " + e1.Message, e1);
                    }
                }

                if ((ShapeStream.Position < ShapeStream.Length) && (FileDataReader.HasRows))
                {

                    try
                    {
                        CurrentRecordIndex++;

                        if (RecordsRead != null)
                        {
                            if (NotifyAfter > 0)
                            {
                                if (CurrentRecordIndex % NotifyAfter == 0)
                                {
                                    RecordsRead(CurrentRecordIndex, TotalRecordCount);
                                }
                            }
                            else
                            {
                                RecordsRead(CurrentRecordIndex, TotalRecordCount);
                            }
                        }

                        if (PercentRead != null)
                        {
                            if (NotifyAfter > 0)
                            {
                                if (CurrentRecordIndex % NotifyAfter == 0)
                                {
                                    PercentRead(PercentShapesStreamed);
                                }
                            }
                            else
                            {
                                PercentRead(PercentShapesStreamed);
                            }
                        }
                    }
                    catch (Exception e2)
                    {
                        if (ShouldAbortOnError)
                        {
                            throw new Exception("Error updating bookeeping: " + e2.Message, e2);
                        }
                    }


                    try
                    {
                        CurrentShapefileRecord = new ShapefileRecord();
                        CurrentShapefileRecord.SteamedBytesRatio = ShapeStream.Position;
                        CurrentShapefileRecord.Shape = new ShapeRecord(PointParsed, UnknownRecord);
                        CurrentShapefileRecord.Shape.Read(ShapeReader);
                        CurrentShapefileRecord.SteamedBytesRatio = (ShapeStream.Position - CurrentShapefileRecord.SteamedBytesRatio) / ShapeStream.Length;

                        if (!String.IsNullOrEmpty(CurrentShapefileRecord.Shape.Error))
                        {
                            string here = "";
                        }
                    }
                    catch (Exception e3)
                    {
                        if (ShouldAbortOnError)
                        {
                            throw new Exception("Error creating current shapefile record: " + e3.Message, e3);
                        }
                    }

                    SqlGeometry sqlGeometry = null;
                    SqlGeometry sqlGeometryProjected = null;
                    SqlGeography sqlGeography = null;

                   

                    object[] temp = null;

                    try
                    {
                        try
                        {
                            CurrentShapefileRecord.DataArray = new object[FileDataReader.FieldCount];
                            if (FileDataReader.Read())
                            {
                                FileDataReader.GetValues(CurrentShapefileRecord.DataArray);
                            }

                            //for (int i = 0; i < CurrentShapefileRecord.DataArray.Length; i++)
                            //{
                            //    CurrentRow[i] = CurrentShapefileRecord.DataArray[i];
                            //}

                            temp = new object[CurrentShapefileRecord.DataArray.Length + 1];
                            Array.Copy(CurrentShapefileRecord.DataArray, temp, CurrentShapefileRecord.DataArray.Length);
                        }
                        catch (Exception e11)
                        {
                            if (ShouldAbortOnError)
                            {
                                throw new Exception("Error copying input data to output row: " + e11.Message, e11);
                            }
                        }

                        try
                        {
                            if (ShouldTrimStringData)
                            {
                                for (int k = 0; k < temp.Length; k++)
                                {
                                    if (temp[k] != null)
                                    {
                                        if (temp[k].GetType() == typeof(String))
                                        {
                                            String orig = (String)temp[k];
                                            temp[k] = orig.Trim();
                                        }
                                    }
                                }
                            }
                        }
                        catch (Exception e9)
                        {
                            if (ShouldAbortOnError)
                            {
                                throw new Exception("Error trimming shape data: " + e9.Message, e9);
                            }
                        }

                        try
                        {
                            if (CurrentShapefileRecord.Shape != null)
                            {

                                if (CurrentShapefileRecord.Shape.ShapeType == ShapeType.Point)
                                {
                                    temp[temp.Length - 1] = KMLGeometryTypes.GetKMLGeometryTypeName(KMLGeometryType.Point);
                                }
                                else if (CurrentShapefileRecord.Shape.ShapeType == ShapeType.Polygon)
                                {
                                    temp[temp.Length - 1] = KMLGeometryTypes.GetKMLGeometryTypeName(KMLGeometryType.Polygon);
                                }
                                else if (CurrentShapefileRecord.Shape.ShapeType == ShapeType.PolyLine)
                                {
                                    temp[temp.Length - 1] = KMLGeometryTypes.GetKMLGeometryTypeName(KMLGeometryType.LineString);
                                }
                            }
                            else
                            {
                                temp[temp.Length - 1] = KMLGeometryTypes.GetKMLGeometryTypeName(KMLGeometryType.Unknown);
                            }

                            CurrentShapefileRecord.DataArray = temp;
                        }
                        catch (Exception e8)
                        {
                            if (ShouldAbortOnError)
                            {
                                throw new Exception("Error setting shape type: " + e8.Message, e8);
                            }
                        }
                    }
                    catch (Exception e4)
                    {
                        if (ShouldAbortOnError)
                        {
                            throw new Exception("Error copying attributes: " + e4.Message, e4);
                        }
                    }

                    //string name = null;
                    //if (GetOrdinal("name00") >= 0)
                    //{
                    //    name = (string)GetValue(GetOrdinal("name00"));
                    //}

                    if (IncludeSqlGeography)
                    {
                        try
                        {
                            if (String.IsNullOrEmpty(CurrentShapefileRecord.Shape.Error))
                            {
                                sqlGeography = CurrentShapefileRecord.Shape.ToUnionSqlGeography("", SRID);
                            }
                            else
                            {
                                string here = "";
                            }
                        }
                        catch (Exception e)
                        {
                            string message = "ExtendedShapefileDataReader - An exception occurred creating the geography: " + e.Message;
                            if (ShouldAbortOnError)
                            {
                                throw new Exception(message, e);
                            }
                        }

                        temp = new object[CurrentShapefileRecord.DataArray.Length + 1];
                        Array.Copy(CurrentShapefileRecord.DataArray, temp, CurrentShapefileRecord.DataArray.Length);
                        temp[temp.Length - 1] = sqlGeography;

                        CurrentShapefileRecord.DataArray = temp;
                    }

                    if (IncludeSqlGeometry)
                    {
                        try
                        {
                            if (String.IsNullOrEmpty(CurrentShapefileRecord.Shape.Error))
                            {
                                sqlGeometry = CurrentShapefileRecord.Shape.ToUnionSqlGeometry("", SRID);
                            }
                        }
                        catch (Exception e)
                        {
                            string message = "ExtendedShapefileDataReader - An exception occurred creating the geometry: " + e.Message;
                            if (ShouldAbortOnError)
                            {
                                throw new Exception(message, e);
                            }
                        }

                        temp = new object[CurrentShapefileRecord.DataArray.Length + 1];
                        Array.Copy(CurrentShapefileRecord.DataArray, temp, CurrentShapefileRecord.DataArray.Length);
                        temp[temp.Length - 1] = sqlGeometry;
                        CurrentShapefileRecord.DataArray = temp;
                    }

                    if (IncludeSoundex)
                    {
                        try
                        {
                            if (SoundexColumns != null)
                            {
                                for (int i = 0; i < SoundexColumns.Length; i++)
                                {
                                    string soundex = "";
                                    string column = SoundexColumns[i];

                                    object origValue = GetValue(GetOrdinal(column));
                                    if (origValue != DBNull.Value)
                                    {
                                        string orig = (string)GetValue(GetOrdinal(column));
                                        soundex = SoundexEncoder.ComputeEncoding(orig);
                                    }
                                    else
                                    {
                                        soundex = "";
                                    }

                                    temp = new object[CurrentShapefileRecord.DataArray.Length + 1];
                                    Array.Copy(CurrentShapefileRecord.DataArray, temp, CurrentShapefileRecord.DataArray.Length);
                                    temp[temp.Length - 1] = soundex;
                                    CurrentShapefileRecord.DataArray = temp;
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            if (ShouldAbortOnError)
                            {
                                throw new Exception("Error writing soundex: " + e.Message, e);
                            }
                        }
                    }

                    if (IncludeSoundexDM)
                    {
                        try
                        {
                            if (SoundexDMColumns != null)
                            {
                                for (int i = 0; i < SoundexDMColumns.Length; i++)
                                {
                                    string soundex = "";

                                    DMSoundexEncoder s = new DMSoundexEncoder();
                                    string column = SoundexDMColumns[i];

                                    object origValue = GetValue(GetOrdinal(column));

                                    if (origValue != DBNull.Value)
                                    {
                                        string orig = (string)GetValue(GetOrdinal(column));

                                        if (!String.IsNullOrEmpty(orig))
                                        {
                                            soundex = s.compute(orig);
                                        }
                                    }
                                    else
                                    {
                                        soundex = "";
                                    }

                                    temp = new object[CurrentShapefileRecord.DataArray.Length + 1];
                                    Array.Copy(CurrentShapefileRecord.DataArray, temp, CurrentShapefileRecord.DataArray.Length);
                                    temp[temp.Length - 1] = soundex;
                                    CurrentShapefileRecord.DataArray = temp;
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            if (ShouldAbortOnError)
                            {
                                throw new Exception("Error writing soundexDM: " + e.Message, e);
                            }
                        }
                    }

                    if (IncludeLineEndPoints)
                    {
                        try
                        {
                            if (String.IsNullOrEmpty(CurrentShapefileRecord.Shape.Error))
                            {
                                if (IncludeLineEndPointsColumns != null)
                                {
                                    double fromLat = 0;
                                    double fromLon = 0;
                                    double toLat = 0;
                                    double toLon = 0;

                                    if (CurrentShapefileRecord.Shape.ShapeType == ShapeType.PolyLine)
                                    {
                                        if (sqlGeometry != null)
                                        {
                                            fromLon = sqlGeometry.STStartPoint().STX.Value;
                                            fromLat = sqlGeometry.STStartPoint().STY.Value;
                                            toLon = sqlGeometry.STEndPoint().STX.Value;
                                            toLat = sqlGeometry.STEndPoint().STY.Value;
                                        }
                                    }

                                    temp = new object[CurrentShapefileRecord.DataArray.Length + 4];
                                    Array.Copy(CurrentShapefileRecord.DataArray, temp, CurrentShapefileRecord.DataArray.Length);

                                    temp[temp.Length - 4] = fromLon;
                                    temp[temp.Length - 3] = fromLat;
                                    temp[temp.Length - 2] = toLon;
                                    temp[temp.Length - 1] = toLat;

                                    CurrentShapefileRecord.DataArray = temp;
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            if (ShouldAbortOnError)
                            {
                                throw new Exception("Error writing EndPoints: " + e.Message, e);
                            }
                        }
                    }

                    if (ShouldSplitAddressRanges)
                    {
                        try
                        {
                            if (SplitAddressRangesColumns != null)
                            {
                                for (int i = 0; i < SplitAddressRangesColumns.Length; i++)
                                {
                                    string column = SplitAddressRangesColumns[i];
                                    string orig = GetString(GetOrdinal(column));

                                    int numberRange = -1;
                                    string unitRange = "";

                                    if (!String.IsNullOrEmpty(orig))
                                    {
                                        if (NumberUtils.IsInt(orig))
                                        {
                                            numberRange = Convert.ToInt32(orig);
                                        }
                                        else
                                        {
                                            if (orig.IndexOf('-') >= 0)
                                            {
                                                string[] parts = orig.Split('-');
                                                if (parts.Length == 2)
                                                {
                                                    if (NumberUtils.IsInt(parts[0]))
                                                    {
                                                        numberRange = Convert.ToInt32(parts[0]);

                                                    }

                                                    unitRange = parts[1];
                                                }
                                            }
                                        }
                                    }


                                    temp = new object[CurrentShapefileRecord.DataArray.Length + 2];
                                    Array.Copy(CurrentShapefileRecord.DataArray, temp, CurrentShapefileRecord.DataArray.Length);

                                    if (numberRange >= 0)
                                    {
                                        temp[temp.Length - 2] = numberRange;
                                    }

                                    temp[temp.Length - 1] = unitRange;
                                    CurrentShapefileRecord.DataArray = temp;
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            if (ShouldAbortOnError)
                            {
                                throw new Exception("Error spliting address range: " + e.Message, e);
                            }
                        }
                    }

                    if (ShouldAddEvenOddFlag)
                    {
                        try
                        {
                            if (AddEvenOddFlagColumns != null)
                            {
                                for (int i = 0; i < AddEvenOddFlagColumns.Length; i++)
                                {

                                    temp = new object[CurrentShapefileRecord.DataArray.Length + 1];
                                    Array.Copy(CurrentShapefileRecord.DataArray, temp, CurrentShapefileRecord.DataArray.Length);

                                    string column = AddEvenOddFlagColumns[i];
                                    int value = GetInt32(GetOrdinal(column));

                                    if (value > 0)
                                    {
                                        int evenOdd = value % 2;
                                        bool odd = Convert.ToBoolean(evenOdd);
                                        temp[temp.Length - 1] = !odd;
                                    }

                                    CurrentShapefileRecord.DataArray = temp;
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            if (ShouldAbortOnError)
                            {
                                throw new Exception("Error in odd even flag: " + e.Message, e);
                            }
                        }
                    }

                    if (ShouldIncludeGeometryProjected)
                    {
                        try
                        {
                            if (sqlGeography != null)
                            {
                                if (!sqlGeography.IsNull && !sqlGeography.STIsEmpty().Value)
                                {
                                    try
                                    {
                                        SqlGeography center = sqlGeography.EnvelopeCenter();
                                        SqlProjection projection = SqlProjection.AlbersEqualArea(96, 40, 20, 60);
                                        sqlGeometryProjected = projection.Project(sqlGeography);
                                    }
                                    catch (Exception e)
                                    {
                                        string message = "ExtendedShapefileDataReader - An exception occurred projecting the geography: " + e.Message;
                                    }
                                }
                            }

                            temp = new object[CurrentShapefileRecord.DataArray.Length + 1];
                            Array.Copy(CurrentShapefileRecord.DataArray, temp, CurrentShapefileRecord.DataArray.Length);
                            temp[temp.Length - 1] = sqlGeometryProjected;
                            CurrentShapefileRecord.DataArray = temp;
                        }
                        catch (Exception e)
                        {
                            if (ShouldAbortOnError)
                            {
                                throw new Exception("Error projecting geometry: " + e.Message, e);
                            }
                        }
                    }

                    if (ShouldIncludeArea)
                    {
                        try
                        {
                            double area = 0;

                            if (sqlGeography != null)
                            {
                                if (!sqlGeography.IsNull && !sqlGeography.STIsEmpty().Value)
                                {
                                    try
                                    {
                                        area = sqlGeography.STArea().Value;
                                    }
                                    catch (Exception e)
                                    {
                                        string message = "ExtendedShapefileDataReader - An exception occurred calculating the area: " + e.Message;
                                        if (ShouldAbortOnError)
                                        {
                                            throw new Exception("message: " + e.Message, e);
                                        }
                                    }
                                }
                            }

                            temp = new object[CurrentShapefileRecord.DataArray.Length + 1];
                            Array.Copy(CurrentShapefileRecord.DataArray, temp, CurrentShapefileRecord.DataArray.Length);
                            temp[temp.Length - 1] = area;
                            CurrentShapefileRecord.DataArray = temp;
                        }
                        catch (Exception e)
                        {
                            if (ShouldAbortOnError)
                            {
                                throw new Exception("Error including area: " + e.Message, e);
                            }
                        }
                    }

                    if (ShouldIncludeCentroid)
                    {
                        try
                        {
                            SqlGeometry centroid = null;
                            double centroidX = 0;
                            double centroidY = 0;

                            if (sqlGeography != null)
                            {
                                if (!sqlGeography.IsNull && !sqlGeography.STIsEmpty().Value)
                                {
                                    try
                                    {
                                        centroid = sqlGeometry.STCentroid();
                                        if (!centroid.IsNull && !centroid.STIsEmpty().Value)
                                        {
                                            centroidX = centroid.STX.Value;
                                            centroidY = centroid.STY.Value;
                                        }
                                    }
                                    catch (Exception e)
                                    {
                                        string message = "ExtendedShapefileDataReader - An exception occurred calculating the centroid: " + e.Message;
                                        if (ShouldAbortOnError)
                                        {
                                            throw new Exception("message: " + e.Message, e);
                                        }
                                    }
                                }
                            }

                            temp = new object[CurrentShapefileRecord.DataArray.Length + 2];
                            Array.Copy(CurrentShapefileRecord.DataArray, temp, CurrentShapefileRecord.DataArray.Length);
                            temp[temp.Length - 2] = centroidX;
                            temp[temp.Length - 1] = centroidY;
                            CurrentShapefileRecord.DataArray = temp;
                        }
                        catch (Exception e)
                        {
                            if (ShouldAbortOnError)
                            {
                                throw new Exception("Error computing centroid: " + e.Message, e);
                            }
                        }
                    }

                }
                else
                {
                    ret = false;
                    //CloseStream();
                }
            }
            catch (Exception e)
            {
                throw new Exception("ExtendedShapefileDataReader - Exception occured in NextFeature: recordIndex: " + CurrentRecordIndex + " - " + e.Message, e);
            }
            return ret;
        }



        #region IDataReader Members


        public override DataTable GetSchemaTable()
        {

            try
            {
                if (SchemaTable == null)
                {

                    string fn = string.Format(@"{0}\{1}.dbf", Path.GetDirectoryName(FileName), Path.GetFileNameWithoutExtension(FileName));
                    if (File.Exists(fn))
                    {
                        string tempDirectory = Directory.GetCurrentDirectory();

                        int bits = IntPtr.Size * 8;

                        if (bits != 32)
                        {
                            throw new Exception("ExtendedShapefileDataReader must be run in 32-Bit mode due to dbf reading components. Recompile as 32-Bit or use the 32-Bit Binaries");
                        }
                        else
                        {

                            TempDfbFile = Path.Combine(tempDirectory, "temp.dbf");
                            File.SetAttributes(fn, FileAttributes.Normal);
                            File.Copy(fn, TempDfbFile, true);
                            File.SetAttributes(TempDfbFile, FileAttributes.Temporary);


                            string connectionString = "Driver={Microsoft dBASE Driver (*.dbf)};DriverID=277;Dbq=" + tempDirectory;
                            FileDataConnection = new OdbcConnection(connectionString);


                            try
                            {
                                FileDataConnection.Open();
                            }
                            catch (Exception e)
                            {
                                throw new Exception("Error occurred opening connection to dbf file: " + e.Message, e);
                            }

                            OdbcCommand cmd = new OdbcCommand();
                            cmd.CommandText = "SELECT count(*) FROM [temp.dbf]";
                            cmd.Connection = FileDataConnection;
                            TotalRecordCount = Convert.ToInt32(cmd.ExecuteScalar());

                            cmd = new OdbcCommand();
                            cmd.CommandText = "SELECT * FROM [temp.dbf]";
                            cmd.Connection = FileDataConnection;
                            FileDataReader = cmd.ExecuteReader();
                            SchemaTable = FileDataReader.GetSchemaTable();


                            // use oleDb
                            //string connectionString = "Provider=Microsoft.Jet.OLEDB.4.0;Extended Properties=dBASE IV;User ID=;Password=;Data Source=" + tempDirectory + ";";
                            //FileDataConnection = new OleDbConnection(connectionString);

                            //OleDbCommand cmd = new OleDbCommand();
                            //cmd.CommandText = "SELECT count(*) FROM [temp.dbf]";
                            //cmd.Connection = FileDataConnection;
                            //TotalRecordCount = Convert.ToInt32(cmd.ExecuteScalar());

                            //cmd = new OleDbCommand();
                            //cmd.CommandText = "SELECT * FROM [temp.dbf]";
                            //cmd.Connection = FileDataConnection;
                            //FileDataReader = cmd.ExecuteReader();
                            //SchemaTable = FileDataReader.GetSchemaTable();
                        }
                    }

                    if (SchemaTable != null)
                    {

                        DataRow row = SchemaTable.NewRow();
                        row["ColumnName"] = "shapeType";
                        row["ColumnOrdinal"] = 0;
                        row["DataType"] = typeof(string);
                        //row["ProviderType"] = 0;
                        row["IsReadOnly"] = true;
                        SchemaTable.Rows.Add(row);


                        if (IncludeSqlGeography)
                        {
                            row = SchemaTable.NewRow();
                            row["ColumnName"] = "shapeGeog";
                            row["ColumnOrdinal"] = 0;
                            row["DataType"] = typeof(SqlGeography);
                            //row["ProviderType"] = 0;
                            row["IsReadOnly"] = true;
                            SchemaTable.Rows.Add(row);
                        }

                        if (IncludeSqlGeometry)
                        {
                            row = SchemaTable.NewRow();
                            row["ColumnName"] = "shapeGeom";
                            row["ColumnOrdinal"] = 0;
                            row["DataType"] = typeof(SqlGeometry);
                            //row["ProviderType"] = typeof(SqlGeometry);
                            row["IsReadOnly"] = true;
                            SchemaTable.Rows.Add(row);
                        }

                        if (IncludeSoundex)
                        {
                            if (SoundexColumns != null)
                            {
                                for (int i = 0; i < SoundexColumns.Length; i++)
                                {
                                    string column = SoundexColumns[i];

                                    row = SchemaTable.NewRow();
                                    row["ColumnName"] = column + "_Soundex";
                                    row["ColumnOrdinal"] = 0;
                                    row["DataType"] = typeof(String);
                                    row["IsReadOnly"] = true;
                                    SchemaTable.Rows.Add(row);
                                }
                            }
                        }

                        if (IncludeSoundexDM)
                        {
                            if (SoundexDMColumns != null)
                            {
                                for (int i = 0; i < SoundexDMColumns.Length; i++)
                                {
                                    string column = SoundexDMColumns[i];

                                    row = SchemaTable.NewRow();
                                    row["ColumnName"] = column + "_SoundexDM";
                                    row["ColumnOrdinal"] = 0;
                                    row["DataType"] = typeof(String);
                                    row["IsReadOnly"] = true;
                                    SchemaTable.Rows.Add(row);
                                }
                            }
                        }

                        if (IncludeLineEndPoints)
                        {
                            if (IncludeLineEndPointsColumns != null)
                            {
                                for (int i = 0; i < IncludeLineEndPointsColumns.Length; i++)
                                {
                                    string column = IncludeLineEndPointsColumns[i];

                                    row = SchemaTable.NewRow();
                                    row["ColumnName"] = column;
                                    row["ColumnOrdinal"] = 0;
                                    row["DataType"] = typeof(Double);
                                    row["IsReadOnly"] = true;
                                    SchemaTable.Rows.Add(row);
                                }
                            }
                        }

                        if (ShouldSplitAddressRanges)
                        {
                            if (SplitAddressRangesColumns != null)
                            {
                                for (int i = 0; i < SplitAddressRangesColumns.Length; i++)
                                {
                                    string column = SplitAddressRangesColumns[i];

                                    row = SchemaTable.NewRow();
                                    row["ColumnName"] = column + "_Number";
                                    row["ColumnOrdinal"] = 0;
                                    row["DataType"] = typeof(Int32);
                                    row["IsReadOnly"] = true;
                                    SchemaTable.Rows.Add(row);

                                    row = SchemaTable.NewRow();
                                    row["ColumnName"] = column + "_Unit";
                                    row["ColumnOrdinal"] = 0;
                                    row["DataType"] = typeof(String);
                                    row["IsReadOnly"] = true;
                                    SchemaTable.Rows.Add(row);
                                }
                            }
                        }

                        if (ShouldAddEvenOddFlag)
                        {
                            if (AddEvenOddFlagColumns != null)
                            {
                                for (int i = 0; i < AddEvenOddFlagColumns.Length; i++)
                                {
                                    string column = AddEvenOddFlagColumns[i];

                                    row = SchemaTable.NewRow();
                                    row["ColumnName"] = column + "_Even";
                                    row["ColumnOrdinal"] = 0;
                                    row["DataType"] = typeof(Boolean);
                                    row["IsReadOnly"] = true;
                                    SchemaTable.Rows.Add(row);
                                }
                            }
                        }

                        if (ShouldIncludeGeometryProjected)
                        {
                            row = SchemaTable.NewRow();
                            row["ColumnName"] = "shapeGeomProjected";
                            row["ColumnOrdinal"] = 0;
                            row["DataType"] = typeof(SqlGeometry);
                            //row["ProviderType"] = typeof(SqlGeometry);
                            row["IsReadOnly"] = true;
                            SchemaTable.Rows.Add(row);
                        }

                        if (ShouldIncludeArea)
                        {
                            row = SchemaTable.NewRow();
                            row["ColumnName"] = "shapeArea";
                            row["ColumnOrdinal"] = 0;
                            row["DataType"] = typeof(Double);
                            row["IsReadOnly"] = true;
                            SchemaTable.Rows.Add(row);
                        }

                        if (ShouldIncludeCentroid)
                        {
                            row = SchemaTable.NewRow();
                            row["ColumnName"] = "CentroidX";
                            row["ColumnOrdinal"] = 0;
                            row["DataType"] = typeof(Double);
                            row["IsReadOnly"] = true;
                            SchemaTable.Rows.Add(row);

                            row = SchemaTable.NewRow();
                            row["ColumnName"] = "CentroidY";
                            row["ColumnOrdinal"] = 0;
                            row["DataType"] = typeof(Double);
                            row["IsReadOnly"] = true;
                            SchemaTable.Rows.Add(row);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("ExtendedShapefileDataReader - Exception occured GetSchemaTable: " + e.Message, e);
            }

            return SchemaTable;
        }


        #endregion

    }
}