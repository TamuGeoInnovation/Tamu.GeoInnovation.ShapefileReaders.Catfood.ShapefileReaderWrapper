using Catfood.Shapefile;
using Microsoft.SqlServer.Types;
using Reimers.Esri;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.IO;
using USC.GISResearchLab.Common.Core.KML;
using USC.GISResearchLab.Common.Databases.DataReaders;
using USC.GISResearchLab.Common.Geometries.Polygons;

namespace USC.GISResearchLab.Common.Shapefiles.ShapefileReaders
{
    /// <summary>
    /// Holds the data contained in an ESRI shapefile.
    /// </summary>
    public class CatfoodShapefileDataReader : AbstractDataReader
    {
        #region Fields


        public string strError { get; set; }

        public BinaryReader ShapeReader { get; set; }
        public Stream ShapeStream { get; set; }

        public new OdbcConnection FileDataConnection { get; set; }
        public new OdbcDataReader FileDataReader { get; set; }

        public int TotalShapes { get; set; }

        public Catfood.Shapefile.Shapefile ShapeFileLayer { get; set; }
        public IEnumerator<Catfood.Shapefile.Shape> ShapeFileRowEnumerator { get; set; }


        #endregion

        #region Events

        /// <summary>
        /// Triggered when a record has been read from the stream.
        /// </summary>
        /// <remarks>
        /// <para>Tie an event handler to this event to be notified when a record has been read from the stream.</para>
        /// </remarks>
        public event PercentReadHandler PercentRead;


        /// <summary>
        /// Triggered when a record has been read from the stream.
        /// </summary>
        /// <remarks>
        /// <para>Tie an event handler to this event to be notified when a record has been read from the stream.</para>
        /// </remarks>
        public event RecordsReadHandler RecordsRead;

        #endregion

        #region Properties

        public Hashtable ColumnOrdinalHashTable { get; set; }
        public bool IncludeSqlGeometry { get; set; }
        public bool IncludeSqlGeography { get; set; }
        public bool IncludeSqlGeographyAsGeoJSON { get; set; }
        public bool IncludeSqlGeometryAsGeoJSON { get; set; }
        public DataTable CurrentDataTable { get; set; }
        public int SRID { get; set; }
        public ShapefileRecord CurrentShapefileRecord { get; set; }

        public override object[] CurrentRow
        {
            get
            {
                object[] ret = null;
                if (CurrentShapefileRecord != null)
                {
                    ret = CurrentShapefileRecord.DataArray;
                }
                return ret;
            }
        }



        /// <summary>
        /// Kaveh: Returns the percentage of shaperecord bytes read from the steam. It will be a double number between 0 to 1.
        /// </summary>
        public double PercentShapesStreamed
        {
            get
            {
                if (ShapeStream == null) return 0.0;
                else return Convert.ToDouble(ShapeStream.Position) / ShapeStream.Length;
            }
        }

        /// <summary>
        /// Gets any error messages for the <see cref="Reimers.Esri.Shapefile"/>.
        /// </summary>        
        public string Error
        {
            get { return strError; }
        }

        #endregion

        #region Constructor

        public CatfoodShapefileDataReader()
        {
            strError = string.Empty;
        }

        /// <summary>
        /// Creates a new instance of a Shapefile
        /// </summary>
        /// <param name="FileName">The path to the .shp file in the shapefile to open.</param>
        public CatfoodShapefileDataReader(string fileName)
        {

            FileName = fileName;

            if (Path.GetExtension(FileName).ToLower() != ".shp")
            {
                throw new Exception("The filename must point to the .shp file in the shapefile");
            }

            strError = string.Empty;
        }

        #endregion



        #region Methods

        #region Public

        /// <summary>
        /// Closes the stream objects and release all resources
        /// </summary>
        public void CloseStream()
        {

            if (ShapeFileRowEnumerator != null)
            {
                ShapeFileRowEnumerator.Dispose();
            }

            if (ShapeFileLayer != null)
            {
                ShapeFileLayer.Close();
                ShapeFileLayer.Dispose();
            }

            IsClosed = true;
        }




        public override bool NextFeature()
        {
            bool ret = true;
            try
            {


                if (CurrentRecordIndex < TotalShapes && ShapeFileRowEnumerator.MoveNext())
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



                    SqlGeometry sqlGeometry = null;
                    SqlGeography sqlGeography = null;



                    object[] temp = null;



                    try
                    {
                        try
                        {
                            CurrentShapefileRecord.DataArray = new object[shape.DataRecord.FieldCount];
                            shape.DataRecord.GetValues(CurrentShapefileRecord.DataArray);
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



        #endregion


        #endregion

        #region IDataReader Members


        public override void Close()
        {
            CloseStream();
            base.Close();
        }

        public override DataTable GetSchemaTable()
        {

            try
            {
                if (SchemaTable == null)
                {

                    string fn = string.Format(@"{0}\{1}.dbf", Path.GetDirectoryName(FileName), Path.GetFileNameWithoutExtension(FileName));
                    if (File.Exists(fn))
                    {
                        int bits = IntPtr.Size * 8;

                        if (bits != 32)
                        {
                            throw new Exception("ExtendedCatfoodDataReader must be run in 32-Bit mode due to dbf reading components. Recompile as 32-Bit or use the 32-Bit Binaries");
                        }
                        else
                        {
                            TempDfbFile = Directory.GetCurrentDirectory() + "\\temp.dbf";
                            File.SetAttributes(fn, FileAttributes.Normal);
                            File.Copy(fn, TempDfbFile, true);
                            File.SetAttributes(TempDfbFile, FileAttributes.Temporary);

                            FileDataConnection = new OdbcConnection(string.Format(@"DBQ={0};Driver={{Microsoft dBase Driver (*.dbf)}}; DriverId=277;FIL=dBase4.0", Directory.GetCurrentDirectory()));
                            FileDataConnection.Open();

                            OdbcCommand cmd = new OdbcCommand();
                            cmd.CommandText = "SELECT count(*) FROM [temp.dbf]";
                            cmd.Connection = FileDataConnection;
                            TotalRecordCount = Convert.ToInt32(cmd.ExecuteScalar());

                            cmd = new OdbcCommand();
                            cmd.CommandText = "SELECT * FROM [temp.dbf]";
                            cmd.Connection = FileDataConnection;
                            FileDataReader = cmd.ExecuteReader();
                            SchemaTable = FileDataReader.GetSchemaTable();



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
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Exception occured GetSchemaTable: " + e.Message, e);
            }

            return SchemaTable;
        }

        #endregion


    }
}