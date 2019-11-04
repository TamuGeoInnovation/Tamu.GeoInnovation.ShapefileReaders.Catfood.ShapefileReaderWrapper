using Catfood.Shapefile;
using Microsoft.SqlServer.Types;
using Reimers.Esri;
using SQLSpatialTools;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.IO;
using USC.GISResearchLab.Common.Core.KML;
using USC.GISResearchLab.Common.Core.TextEncodings.Soundex;
using USC.GISResearchLab.Common.Geometries.Polygons;

namespace USC.GISResearchLab.Common.Shapefiles.ShapefileReaders
{
    /// <summary>
    /// Holds the data contained in an ESRI shapefile.
    /// </summary>
    public class ExtendedSharpMapShapefileDataReader : ShapefileDataReader
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

        public int TotalShapes { get; set; }

        Catfood.Shapefile.Shapefile ShapeFileLayer { get; set; }
        IEnumerator<Catfood.Shapefile.Shape> ShapeFileRowEnumerator { get; set; }

        #endregion

        #region Constructor

        public ExtendedSharpMapShapefileDataReader()
        {
        }


        public ExtendedSharpMapShapefileDataReader(string fileName)
            : base(fileName)
        {

            ShapeFileLayer = new Catfood.Shapefile.Shapefile(fileName);
            ShapeFileRowEnumerator = ShapeFileLayer.GetEnumerator();


            TotalShapes = ShapeFileLayer.Count;



        }

        #endregion

        public override bool NextFeature()
        {
            bool ret = true;
            try
            {

                CurrentShapefileRecord = null;

                if (CurrentRecordIndex < TotalShapes && (FileDataReader.HasRows) && ShapeFileRowEnumerator.MoveNext())
                {
                    CurrentRecordIndex++;


                    Shape shape = ShapeFileRowEnumerator.Current;
                    try
                    {

                        if (TotalShapes != TotalRecordCount)
                        {
                            throw new Exception("Error reading shapefile and DBF - record counts do no match. Total shapes: " + TotalShapes + ", total records: " + TotalRecordCount);
                        }


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


                    CurrentShapefileRecord = new ShapefileRecord();
                    //CurrentShapefileRecord.SteamedBytesRatio = ShapeStream.Position;
                    //CurrentShapefileRecord.Shape = new ShapeRecord(PointParsed, UnknownRecord);
                    //CurrentShapefileRecord.Shape.Read(ShapeReader);
                    //CurrentShapefileRecord.SteamedBytesRatio = (ShapeStream.Position - CurrentShapefileRecord.SteamedBytesRatio) / ShapeStream.Length;



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



                            temp[temp.Length - 1] = KMLGeometryTypes.GetKMLGeometryTypeName(KMLGeometryType.Polygon);


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


                    Polygon poly = null;

                    List<SqlGeography> geographies = new List<SqlGeography>();

                    if (IncludeSqlGeography)
                    {
                        try
                        {


                            ShapePolygon orig = (ShapePolygon)shape;

                            foreach (PointD[] points in orig.Parts)
                            {
                                poly = new Polygon();
                                foreach (PointD point in points)
                                {
                                    poly.AddPoint(point.X, point.Y);
                                }

                                SqlGeography sqlGeog = poly.ToSqlGeography(SRID);
                                geographies.Add(sqlGeog);
                            }

                            sqlGeography = geographies[0];


                            if (geographies.Count > 1)
                            {

                                for (int i = 1; i < geographies.Count; i++)
                                {
                                    sqlGeography = sqlGeography.STUnion(geographies[i]);
                                }
                            }



                            //SqlGeographyBuilder gb = new SqlGeographyBuilder();
                            //gb.SetSrid(SRID);

                            //if (orig.Parts.Count > 1)
                            //{
                            //    gb.BeginGeography(OpenGisGeographyType.MultiPolygon);
                            //}

                            //foreach (PointD[] points in orig.Parts)
                            //{
                            //    gb.BeginGeography(OpenGisGeographyType.Polygon);

                            //    double lon = points[0].X;
                            //    double lat = points[0].Y;
                            //    gb.BeginFigure(lat, lon);

                            //    double lastLon = 0;
                            //    double lastLat = 0;

                            //    for (int i = 1; i < points.Length; i++)
                            //    {
                            //        lastLon = points[i].X;
                            //        lastLat = points[i].Y;

                            //        gb.AddLine(lastLat, lastLon);
                            //    }

                            //    // add the first points back in
                            //    gb.AddLine(lat, lon);

                            //    gb.EndFigure();
                            //    gb.EndGeography();
                            //}

                            //if (orig.Parts.Count > 1)
                            //{
                            //    gb.EndGeography();
                            //}

                            //string geog = gb.ToString();

                            //sqlGeography = gb.ConstructedGeography;

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

                            List<SqlGeometry> sqlGeometries = new List<SqlGeometry>();

                            ShapePolygon orig = (ShapePolygon)shape;

                            foreach (PointD[] points in orig.Parts)
                            {
                                poly = new Polygon();
                                foreach (PointD point in points)
                                {
                                    poly.AddPoint(point.X, point.Y);
                                }

                                SqlGeometry sqlGeom = poly.ToSqlGeometry(SRID);
                                sqlGeometries.Add(sqlGeom);
                            }

                            sqlGeometry = sqlGeometries[0];


                            if (sqlGeometries.Count > 1)
                            {

                                for (int i = 1; i < sqlGeometries.Count; i++)
                                {
                                    sqlGeometry = sqlGeometry.STUnion(sqlGeometries[i]);
                                }
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
                    ShapeFileLayer.Close();
                    ShapeFileLayer.Dispose();

                }
            }
            catch (Exception e)
            {
                ShapeFileLayer.Close();
                ShapeFileLayer.Dispose();
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