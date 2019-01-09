package io

import java.io.{File, RandomAccessFile}

import common.{CatalogHelper, Constants, Utility}
import datatype._
import datatype.base.DTypeNum
import io.model.{BaseCondition, Page, RecordData, RecordPointer}
import scala.util.control.Breaks.break

import scala.collection.mutable

class IOHandler {

  def createTable(database: String, table: String): Boolean = {
    try {
      val directory = new File(Utility.getDatabasePath(database))
      if (!directory.exists()) directory.mkdir()
      val file = new File(Utility.getDatabasePath(database) + "/" + table + Constants.DefaultFileExtension)
      if (!file.exists()) return false
      if (file.createNewFile()) {
        val page = Page.createEmptyPage[RecordData]
        val randomAccessFile = new RandomAccessFile(file, "rw")
        randomAccessFile.setLength(Constants.PageSize)
        val tableCreated = writePageHeader(randomAccessFile, page)
        randomAccessFile.close()
        return tableCreated
      }
      false
    } catch {
      case e: Exception =>
        println("Error : " + e.getMessage)
        false
    }
  }

  def writeRecord(database: String, table: String, record: RecordData): Int = {
    val file = new File(Utility.getDatabasePath(database) + "/" + table + Constants.DefaultFileExtension)
    if (file.exists()) {
      val rf = new RandomAccessFile(file, "rw")
      val pg = getPage(rf, record, 0)
      if (pg == null) return -1
      if (!willItFit(pg, record)) {
        val pageCount = rf.length() / Constants.PageSize
        pageCount match {
          case 0 =>
          case 1 =>
            val recPointer = splitPage(rf, pg, record, 1, 2)
            val recPtPage: Page[RecordPointer] = Page.createEmptyPage[RecordPointer]
            recPtPage.number = 0
            recPtPage.pageType = Constants.InteriorTablePage
            recPtPage.cellCount = 1
            recPtPage.startAddress = (recPtPage.startAddress - RecordPointer.size).toShort
            recPtPage.rightNodeAddress = 2
            val tmp = recPtPage.startAddress + 1
            recPtPage.listOfRecordAddress.+=(tmp.toShort)
            writePageHeader(rf, recPtPage)
            writeRecord(rf, recPointer)
          case _ =>
            val recPtr = splitPage(rf, readPageHeader(rf, 0), record)
            if (recPtr != null && recPtr.leftPage != -1) {
              val rootPg = Page.createEmptyPage[RecordPointer]
              rootPg.number = 0
              rootPg.pageType = Constants.InteriorTablePage
              rootPg.cellCount = 1
              rootPg.startAddress = (rootPg.startAddress - RecordPointer.size).toShort
              rootPg.rightNodeAddress = recPtr.page
              val tmp = rootPg.startAddress + 1
              rootPg.listOfRecordAddress.+=(tmp.toShort)
              recPtr.offset = (rootPg.startAddress + 1).toShort
              writePageHeader(rf, rootPg)
              writeRecord(rf, recPtr)
            }
        }
        CatalogHelper.incRowCount(database, table)
        rf.close()
        return record.offset
      }

      val address = fetchAddress(file, record.rowId, pg.number)
      pg.cellCount = (pg.cellCount + 1).toByte
      pg.startAddress = (pg.startAddress - record.size - record.headerSize).toShort
      if (address == pg.listOfRecordAddress.size)
        pg.listOfRecordAddress.+=((pg.startAddress + 1).toShort)
      else
        insertAtIndex(pg.listOfRecordAddress, address, (pg.startAddress + 1).toShort)
      record.page = pg.number
      record.offset = (pg.startAddress + 1).toShort
      writePageHeader(rf, pg)
      writeRecord(rf, record)
      CatalogHelper.incRowCount(database, table)
      rf.close()
    } else
      println(s"Table $table does not exist")

    record.offset
  }

  def insertAtIndex[T](list: mutable.MutableList[T], i: Int, value: T) = {
    list.take(i) ++ List(value) ++ list.drop(i)
  }

  def fetchAddress(f: File, rowId: Int, pageNum: Int): Int = {
    var loc = -1
    val rf = new RandomAccessFile(f, "r")
    val pg = readPageHeader(rf, pageNum)
    if (pg.pageType == Constants.LeafTablePage) {
      loc = binarySearch(rf, rowId, pg.cellCount, pg.number * Constants.PageSize + Page.fixedHeaderLength, pg.pageType)
      rf.close()
    }
    loc
  }

  def binarySearch(rf: RandomAccessFile, key: Int, recCount: Int, seekPos: Long, pageType: Byte) = {
    binarySearch(rf, key, recCount, seekPos, pageType, false)
  }

  def binarySearch(rf: RandomAccessFile, key: Int, recCount: Int, seekPos: Long, pageType: Byte, literalSearch: Boolean) = ???

  def writeRecord(rf: RandomAccessFile, recPtr: RecordPointer) = {
    rf.seek(recPtr.page * Constants.PageSize + recPtr.offset)
    rf.writeInt(recPtr.leftPage)
    rf.writeInt(recPtr.key)
  }

  def writeRecord(file: RandomAccessFile, data: RecordData): Boolean = ???

  def willItFit(pg: Page[RecordData], rec: RecordData): Boolean = {
    if (pg != null && rec != null) {
      val finalAddress = pg.startAddress
      val begAdd = Page.fixedHeaderLength + pg.listOfRecordAddress.size * java.lang.Short.BYTES
      return (rec.size + rec.headerSize + java.lang.Short.BYTES) <= (finalAddress - begAdd)
    }
    false
  }

  def splitPage(rf: RandomAccessFile, pg: Page[RecordData], rec: RecordData, p1: Int, p2: Int): RecordPointer = {
    if (pg != null && rec != null) {
      if (pg.pageType == Constants.InteriorTablePage) return null
      val loc : Int = binarySearch(rf, rec.rowId, pg.cellCount, pg.number * Constants.PageSize + Page.fixedHeaderLength, pg.pageType)
      rf.setLength(Constants.PageSize * (p2 + 1))
      val recordPointer = RecordPointer(-1, -1, -1, -1)
      if (loc == pg.cellCount) {
        val pg1 = Page.createEmptyPage(p1)
        pg1.pageType = pg.pageType
        pg1.cellCount = pg.cellCount
        pg1.rightNodeAddress = p2
        pg1.startAddress = pg.startAddress
        pg1.listOfRecordAddress = pg.listOfRecordAddress
        writePageHeader(rf, pg1)
        val recordList = copyRecords(rf, pg.number * Constants.PageSize, pg.listOfRecordAddress, 0, pg.cellCount, p1, rec)
        recordList.map(writeRecord(rf, _))

        val pg2 = Page.createEmptyPage[RecordData](p2)
        pg2.pageType = pg.pageType
        pg2.cellCount = 1
        pg2.rightNodeAddress = pg.rightNodeAddress
        pg2.startAddress = (Constants.PageSize - 1 - rec.size - rec.headerSize).toShort
        pg2.listOfRecordAddress += (pg2.startAddress + 1).toShort
        writePageHeader(rf, pg2)
        rec.page = pg2.number
        rec.offset = (pg2.startAddress + 1).toShort
        writeRecord(rf, rec)
        recordPointer.key = rec.rowId
      } else {
        var initial = false
        if(loc < pg.listOfRecordAddress.size / 2) initial = true
        rf.setLength(Constants.PageSize * (p2 + 1))

        val pg1 = Page.createEmptyPage(p1)
        pg1.pageType = pg.pageType
        val leftRec = copyRecords(rf, pg.number * Constants.PageSize, pg.listOfRecordAddress, 0, (pg.cellCount / 2).toByte, p1, rec)
        if(initial){
          rec.page = p1
          insertAtIndex(leftRec, loc, rec)
        }
        pg1.cellCount = leftRec.size.toByte
        var offset : Short = 0
        (1 to leftRec.size).foreach { i =>
          offset = (Constants.PageSize - (leftRec(i-1).size + leftRec(i-1).headerSize) * i).toShort
          leftRec(i-1).offset = offset
          pg1.listOfRecordAddress.+=(offset)
        }
        pg1.startAddress = (offset - 1).toShort
        pg1.rightNodeAddress = p2
        writePageHeader(rf, pg1)
        leftRec.foreach(writeRecord(rf, _))


        val pg2 = Page.createEmptyPage(p2)
        pg2.pageType = pg.pageType
        val rightRec = copyRecords(rf, pg.number * Constants.PageSize, pg.listOfRecordAddress, (pg.cellCount / 2 + 1).toByte, pg.cellCount, p1, rec)
        if(!initial){
          rec.page = p2
          if(loc - (pg.listOfRecordAddress.size / 2) + 1 >= rightRec.size)
            rightRec.+=(rec)
          else
            insertAtIndex(rightRec, loc, rec)
        }
        pg2.cellCount = rightRec.size.toByte
        (1 to rightRec.size).foreach { i =>
          offset = (Constants.PageSize - (rightRec(i-1).size + rightRec(i-1).headerSize) * i).toShort
          rightRec(i-1).offset = offset
          pg2.listOfRecordAddress.+=(offset)
        }

        recordPointer.key = rightRec.head.rowId
        pg2.startAddress = (offset - 1).toShort
        pg2.rightNodeAddress = pg.rightNodeAddress
        writePageHeader(rf, pg2)
        rightRec.foreach(writeRecord(rf, _))
      }
      recordPointer.leftPage = p1
      recordPointer
    } else
        null
  }

  def copyRecords[T](rf: RandomAccessFile, pgBegAdd: Long, recAdd: mutable.MutableList[Short], bInd: Byte, eInd: Byte, pNum: Int, obj: T): mutable.MutableList[T] = {
    val list = mutable.MutableList[T]()
    (bInd to eInd).foreach { i =>
      rf.seek(pgBegAdd + recAdd(i))
      obj.isInstanceOf[RecordPointer] match {
        case true =>
          val offset = pgBegAdd + Constants.PageSize - 1 - (RecordPointer.size * (i - bInd + 1))
          val recordPointer = RecordPointer(rf.readInt(), rf.readInt(), pNum, offset.toShort)
          insertAtIndex(list, i - bInd, recordPointer)
        case false =>
          val rec = RecordData(mutable.MutableList[Object](), rf.readShort(), rf.readInt(), pNum, recAdd(i))
          val recCount = rf.readByte()
          val serialByteCodes = mutable.MutableList[Byte]()
          (0 to recCount).foreach(serialByteCodes += rf.readByte())
          (0 to recCount).foreach { i =>
            serialByteCodes(i) match {
              case Constants.OneByteNullSerialTypeCode => rec.columnValues.+=(new TextDT(null))
              case Constants.TwoByteNullSerialTypeCode => rec.columnValues.+=(new SmallIntDT[Short](rf.readShort(), true))
              case Constants.FourByteNullSerialTypeCode => rec.columnValues.+=(new RealDT[Float](rf.readFloat(), true))
              case Constants.EightByteNullSerialTypeCode => rec.columnValues.+=(new DoubleDT[Double](rf.readDouble(), true))
              case Constants.TinyIntSerialTypeCode => rec.columnValues.+=(new TinyIntDT[Byte](rf.readByte(), false))
              case Constants.SmallIntSerialTypeCode => rec.columnValues.+=(new SmallIntDT[Short](rf.readShort(), false))
              case Constants.IntSerialTypeCode => rec.columnValues.+=(new IntDT[Int](rf.readInt(), false))
              case Constants.BigIntSerialTypeCode => rec.columnValues.+=(new BigIntDT[Byte](rf.readLong(), false))
              case Constants.RealSerialTypeCode => rec.columnValues.+=(new RealDT[Float](rf.readFloat(), false))
              case Constants.DoubleSerialTypeCode => rec.columnValues.+=(new DoubleDT[Double](rf.readDouble(), false))
              case Constants.DateSerialTypeCode => rec.columnValues.+=(new DateDT[Long](rf.readLong(), false))
              case Constants.DateTimeSerialTypeCode => rec.columnValues.+=(new DateTimeDT[Long](rf.readLong(), false))
              case _ =>
                if (serialByteCodes(i) > Constants.TextSerialTypeCode) {
                  val len = (serialByteCodes(i) - Constants.TextSerialTypeCode).toByte
                  val charList = mutable.MutableList[Char]()
                  (0 to len).foreach(charList.+=(rf.readByte().toChar))
                  rec.columnValues.+=(new TextDT(charList.toString()))
                }
            }
          }
          insertAtIndex(list, i - bInd, rec)
      }
    }

    list
  }

  def splitPage(rf: RandomAccessFile, pg: Page[_], rec: RecordData): RecordPointer = ???

  def getPage(rf: RandomAccessFile, record: RecordData, pageNum: Int): Page[RecordData] = ???

  def writePageHeader(rf: RandomAccessFile, pg: Page[_]): Boolean = {
    rf.seek(pg.number * Constants.PageSize)
    rf.writeByte(pg.pageType)
    rf.writeByte(pg.cellCount)
    rf.writeShort(pg.startAddress)
    rf.writeInt(pg.rightNodeAddress)
    pg.listOfRecordAddress.foreach(e => rf.writeShort(e))
    true
  }

  def findRecord(database: String, table: String, condition: BaseCondition, fetchOne: Boolean): mutable.MutableList[RecordData] =
    findRecord(database, table, mutable.MutableList[BaseCondition](condition), null, fetchOne)

  def findRecord(database: String, table: String, conditionList: mutable.MutableList[BaseCondition],
                 fetchOne: Boolean): mutable.MutableList[RecordData] = findRecord(database, table, conditionList, null, fetchOne)

  def findRecord(database: String, table: String, conditionList: mutable.MutableList[BaseCondition],
                 listOfColInd: mutable.MutableList[Byte], fetchOne: Boolean): mutable.MutableList[RecordData] = {

    var recordList = mutable.MutableList[RecordData]()
    val file = new File(Utility.getDatabasePath(database) + "/" + table + Constants.DefaultFileExtension)

    if (file.exists()) {
      val rf = new RandomAccessFile(file, "rw")
      if(conditionList != null){
        var pg : Page[_] = firstLeafPage(file)
        while(pg != null){
          pg.listOfRecordAddress.foreach { address =>
            var isAMatch = false
            val rec = readDataRecord(rf, pg.number, address)
            conditionList.foreach { c =>
              val colIndex = c.index
              val value = c.value
              val condition = c.condition
              if(rec != null && rec.columnValues.size > colIndex){
                isAMatch = compare(rec.columnValues(colIndex), value, condition)
                if(!isAMatch) break()
              }
            }
            if(isAMatch){
              val recordData = RecordData()
              if(listOfColInd != null){
                recordData.rowId = rec.rowId
                recordData.page = rec.page
                recordData.offset = rec.offset
                listOfColInd.foreach(e => recordData.columnValues.+=(rec.columnValues(e)))
              }else
                recordList.+=(rec)
              if(fetchOne){
                rf.close()
                return recordList
              }
            }
          }
          if(pg.rightNodeAddress == Constants.RightMostPage) break()
          pg = readPageHeader(rf, pg.rightNodeAddress)
        }
        rf.close()
        return recordList
      }
    }else
      println(s"Table '$database.$table' doesn't exist.")
    null
  }

  def getLastRecordAndPage(database: String, table: String) : Page[RecordData] = {
    val file = new File(Utility.getDatabasePath(database) + "/" + table + Constants.DefaultFileExtension)
    if(file.exists()) {
      val rf = new RandomAccessFile(file, "r")
      val pg : Page[RecordData] = rightMostLeafPage(file)
      if(pg.cellCount > 0){
        rf.seek(Constants.PageSize * pg.number + Page.fixedHeaderLength + (pg.cellCount - 1) * java.lang.Byte.BYTES)
        val address : Short = rf.readShort()
        val rec = readDataRecord(rf, pg.number, address)
        if(rec != null) pg.records.+=(rec)
      }
      rf.close()
      return pg
    }else
      println(s"Table $database.$table not found")
    null
  }

  def updateRecord(database: String, table: String, condition: BaseCondition, updateListOfColInd: mutable.MutableList[Byte]
                   , updateListOfColVal: mutable.MutableList[Object], isIncrement: Boolean) : Int = {
    val list = mutable.MutableList[BaseCondition]()
    list.+=(condition)
    updateRecord(database, table, list, updateListOfColInd, updateListOfColVal, isIncrement)
  }

  def updateRecord(database: String, table: String, conditions: mutable.MutableList[BaseCondition], updateColInd: mutable.MutableList[Byte],
                   updateColVal: mutable.MutableList[Object], isInc: Boolean) : Int = {

    var recUpdated = 0
    if(conditions == null || updateColInd == null || updateColVal == null || updateColInd.size != updateColVal.size)
      return 0

    val file = new File(Utility.getDatabasePath(database) + "/" + table + Constants.DefaultFileExtension)
    if(file.exists()){
      val rec = findRecord(database, table, conditions, false)
      if(rec != null && rec.nonEmpty){
        val rf = new RandomAccessFile(file, "rw")
        rec.foreach { r =>
          (0 to updateColVal.size).foreach { ind =>
            val index = updateColInd(ind)
            val obj = updateColVal(ind)
            if(isInc) r.columnValues(index) = increment(r.columnValues(index).asInstanceOf[DTypeNum[_]], obj.asInstanceOf[DTypeNum[_]])
            else r.columnValues(index) = obj
          }
          writeRecord(rf, r)
          recUpdated += 1
        }

        rf.close()
        return recUpdated
      }
    }else
      println(s"Table $database.$table not found")

    0
  }

  def increment[T](obj1: DTypeNum[T], obj2: DTypeNum[T]) = {
    obj1.increment(obj2.value)
    obj1
  }

  def checkIfDbExists(database: String) = {
    new File(Utility.getDatabasePath(database)).exists()
  }

  def checkIfTableExists(database: String, table: String) = {
    val dbExist = checkIfDbExists(database)
    if (dbExist) {
      new File(Utility.getDatabasePath(database) + "/" + table + Constants.DefaultFileExtension).exists()
    } else
      false
  }

  def readPageHeader(rf: RandomAccessFile, pageNum: Int): Page[_] = {
    rf.seek(Constants.PageSize * pageNum)
    val pageType = rf.readByte()
    val page : Page[_] = pageType match {
      case Constants.InteriorTablePage => Page[RecordPointer](pageType, rf.readByte(), rf.readShort(), rf.readInt(), mutable.MutableList[Short](), null, pageNum)
      case _ => Page[RecordData](pageType, rf.readByte(), rf.readShort(), rf.readInt(), mutable.MutableList[Short](), null, pageNum)
    }

    (0 to page.cellCount).foreach(page.listOfRecordAddress.+=(rf.readShort()))
    page
  }

  def readDataRecord(rf: RandomAccessFile, pNum: Int, address: Short): RecordData = {
    val rec = RecordData(mutable.MutableList[Object](), rf.readShort(), rf.readInt(), pNum, address)
    val colCount = rf.readByte()
    val serialByteCodes = mutable.MutableList[Byte]()
    (0 to colCount).foreach(serialByteCodes += rf.readByte())
    (0 to colCount).foreach { i =>
      serialByteCodes(i) match {
        case Constants.OneByteNullSerialTypeCode => rec.columnValues.+=(new TextDT(null))
        case Constants.TwoByteNullSerialTypeCode => rec.columnValues.+=(new SmallIntDT[Short](rf.readShort(), true))
        case Constants.FourByteNullSerialTypeCode => rec.columnValues.+=(new RealDT[Float](rf.readFloat(), true))
        case Constants.EightByteNullSerialTypeCode => rec.columnValues.+=(new DoubleDT[Double](rf.readDouble(), true))
        case Constants.TinyIntSerialTypeCode => rec.columnValues.+=(new TinyIntDT[Byte](rf.readByte(), false))
        case Constants.SmallIntSerialTypeCode => rec.columnValues.+=(new SmallIntDT[Short](rf.readShort(), false))
        case Constants.IntSerialTypeCode => rec.columnValues.+=(new IntDT[Int](rf.readInt(), false))
        case Constants.BigIntSerialTypeCode => rec.columnValues.+=(new BigIntDT[Byte](rf.readLong(), false))
        case Constants.RealSerialTypeCode => rec.columnValues.+=(new RealDT[Float](rf.readFloat(), false))
        case Constants.DoubleSerialTypeCode => rec.columnValues.+=(new DoubleDT[Double](rf.readDouble(), false))
        case Constants.DateSerialTypeCode => rec.columnValues.+=(new DateDT[Long](rf.readLong(), false))
        case Constants.DateTimeSerialTypeCode => rec.columnValues.+=(new DateTimeDT[Long](rf.readLong(), false))
        case _ =>
          if (serialByteCodes(i) > Constants.TextSerialTypeCode) {
            val len = (serialByteCodes(i) - Constants.TextSerialTypeCode).toByte
            val charList = mutable.MutableList[Char]()
            (0 to len).foreach(charList.+=(rf.readByte().toChar))
            rec.columnValues.+=(new TextDT(charList.toString()))
          }
      }
    }
    rec
  }

  def compare(ob1: Object, ob2: Object, comparator: Short) : Boolean = {
    var isAMatch = false
    if(ob1 != null){
      Utility.validateClass(ob1) match {
        case Constants.TinyInt =>
          Utility.validateClass(ob2) match {
            case Constants.TinyInt => isAMatch = ob1.asInstanceOf[TinyIntDT].compare(ob2.asInstanceOf[TinyIntDT], comparator)
            case Constants.SmallInt => isAMatch = ob1.asInstanceOf[TinyIntDT].compare(ob2.asInstanceOf[SmallIntDT[Short]], comparator)
            case Constants.Int => isAMatch = ob1.asInstanceOf[TinyIntDT].compare(ob2.asInstanceOf[IntDT[Int]], comparator)
            case Constants.BigInt => isAMatch = ob1.asInstanceOf[TinyIntDT].compare(ob2.asInstanceOf[BigIntDT[Long]], comparator)
            case _ =>
          }

        case Constants.SmallInt =>
          Utility.validateClass(ob2) match {
            case Constants.TinyInt => isAMatch = ob1.asInstanceOf[SmallIntDT].compare(ob2.asInstanceOf[TinyIntDT[Byte]], comparator)
            case Constants.SmallInt => isAMatch = ob1.asInstanceOf[SmallIntDT].compare(ob2.asInstanceOf[SmallIntDT], comparator)
            case Constants.Int => isAMatch = ob1.asInstanceOf[SmallIntDT].compare(ob2.asInstanceOf[IntDT[Int]], comparator)
            case Constants.BigInt => isAMatch = ob1.asInstanceOf[SmallIntDT].compare(ob2.asInstanceOf[BigIntDT[Long]], comparator)
            case _ =>
          }

        case Constants.Int =>
          Utility.validateClass(ob2) match {
            case Constants.TinyInt => isAMatch = ob1.asInstanceOf[IntDT].compare(ob2.asInstanceOf[TinyIntDT[Byte]], comparator)
            case Constants.SmallInt => isAMatch = ob1.asInstanceOf[IntDT].compare(ob2.asInstanceOf[SmallIntDT[Short]], comparator)
            case Constants.Int => isAMatch = ob1.asInstanceOf[IntDT].compare(ob2.asInstanceOf[IntDT], comparator)
            case Constants.BigInt => isAMatch = ob1.asInstanceOf[IntDT].compare(ob2.asInstanceOf[BigIntDT[Long]], comparator)
            case _ =>
          }

        case Constants.BigInt =>
          Utility.validateClass(ob2) match {
            case Constants.TinyInt => isAMatch = ob1.asInstanceOf[BigIntDT].compare(ob2.asInstanceOf[TinyIntDT[Byte]], comparator)
            case Constants.SmallInt => isAMatch = ob1.asInstanceOf[BigIntDT].compare(ob2.asInstanceOf[SmallIntDT[Short]], comparator)
            case Constants.Int => isAMatch = ob1.asInstanceOf[BigIntDT].compare(ob2.asInstanceOf[IntDT[Int]], comparator)
            case Constants.BigInt => isAMatch = ob1.asInstanceOf[BigIntDT].compare(ob2.asInstanceOf[BigIntDT], comparator)
            case _ =>
          }

        case Constants.Real =>
          Utility.validateClass(ob2) match {
            case Constants.Real => isAMatch = ob1.asInstanceOf[RealDT].compare(ob2.asInstanceOf[RealDT], comparator)
            case Constants.Double => isAMatch = ob1.asInstanceOf[RealDT].compare(ob2.asInstanceOf[DoubleDT[Double]], comparator)
            case _ =>
          }

        case Constants.Double =>
          Utility.validateClass(ob2) match {
            case Constants.Real => isAMatch = ob1.asInstanceOf[DoubleDT].compare(ob2.asInstanceOf[RealDT[Float]], comparator)
            case Constants.Double => isAMatch = ob1.asInstanceOf[DoubleDT].compare(ob2.asInstanceOf[DoubleDT], comparator)
            case _ =>
          }

        case Constants.Date =>
          Utility.validateClass(ob2) match {
            case Constants.Date => isAMatch = ob1.asInstanceOf[DateDT].compare(ob2.asInstanceOf[DateDT], comparator)
            case _ =>
          }

        case Constants.DateTime =>
          Utility.validateClass(ob2) match {
            case Constants.DateTime => isAMatch = ob1.asInstanceOf[DateTimeDT].compare(ob2.asInstanceOf[DateTimeDT], comparator)
            case _ =>
          }

        case Constants.Text =>
          Utility.validateClass(ob2) match {
            case Constants.Text => isAMatch = ob1.asInstanceOf[TextDT].value.equalsIgnoreCase(ob2.asInstanceOf[TextDT].value)
          }
      }
    }
    isAMatch
  }

  def deleteRecord(database: String, table: String, conditionList: mutable.MutableList[BaseCondition]) : Int = {
    var numberOfDeletions = 0
    val file = new File(Utility.getDatabasePath(database) + "/" + table + Constants.DefaultFileExtension)

    if (file.exists()) {
      val rf = new RandomAccessFile(file, "rw")
      if(conditionList != null){
        var pg : Page[_] = firstLeafPage(file)
        while(pg != null){
          pg.listOfRecordAddress.foreach { address =>
            var isAMatch = false
            val rec = readDataRecord(rf, pg.number, address)
            conditionList.foreach { c =>
              val colIndex = c.index
              val value = c.value
              val condition = c.condition
              if(rec != null && rec.columnValues.size > colIndex){
                isAMatch = compare(rec.columnValues(colIndex), value, condition)
                if(!isAMatch) break()
              }
            }
            if(isAMatch){
              pg.cellCount = (pg.cellCount - 1).toByte
              pg.listOfRecordAddress.drop(address)
              if(pg.cellCount == 0) pg.startAddress = (pg.number * Constants.PageSize + Constants.PageSize - 1).toShort
              writePageHeader(rf, pg)
              CatalogHelper.decRowCount(database, table)
              numberOfDeletions += 1
            }
          }
          if(pg.rightNodeAddress == Constants.RightMostPage) break()
          pg = readPageHeader(rf, pg.rightNodeAddress)
        }
        rf.close()
        return numberOfDeletions
      }
    }else
      println(s"Table '$database.$table' doesn't exist.")

    0
  }

  def firstLeafPage(file: File) : Page[RecordData] = {
    val rf = new RandomAccessFile(file, "r")
    var pg = readPageHeader(rf, 0)
    while (pg.pageType == Constants.InteriorTablePage){
      if(pg.cellCount == 0) return null
      rf.seek(Constants.PageSize * pg.number + pg.listOfRecordAddress.head)
      pg = readPageHeader(rf, rf.readInt())
    }
    rf.close()
    pg.asInstanceOf[Page[RecordData]]
  }

  def rightMostLeafPage(file: File) : Page[RecordData] = {
    val rf = new RandomAccessFile(file, "r")
    var pg = readPageHeader(rf, 0)
    while (pg.pageType == Constants.InteriorTablePage && pg.rightNodeAddress != Constants.RightMostPage){
      pg = readPageHeader(rf, pg.rightNodeAddress)
    }
    rf.close()
    pg.asInstanceOf[Page[RecordData]]
  }

  def lastRecordAndPage(database: String, table: String) = {

    val file = new File(Utility.getDatabasePath(database) + "/" + table + Constants.DefaultFileExtension)

    if (file.exists()) {
      val randomAccessFile = new RandomAccessFile(file, "r")
      val page = rightMostLeafPage(file)
      if (page.cellCount > 0) {
        randomAccessFile.seek((Constants.PageSize * page.number) + Page.fixedHeaderLength + ((page.cellCount - 1) * java.lang.Short.BYTES))
        val address = randomAccessFile.readShort()
        val record = readDataRecord(randomAccessFile, page.number, address)
        if (record != null)
          page.records.+=(record)
      }
      randomAccessFile.close()
      page
    } else {
      println(s"Table '$database.$table' doesn't exist.")
      null
    }
  }
}