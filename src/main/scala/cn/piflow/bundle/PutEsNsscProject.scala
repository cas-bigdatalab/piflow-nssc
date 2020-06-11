package cn.piflow.bundle

import java.text.SimpleDateFormat
import java.util.Date

import cn.piflow.bundle.bean.Project
import cn.piflow.bundle.entity.{Journal, _}
import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.conf.{ConfigurableStop, Port, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import com.alibaba.fastjson.JSON
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark._


class PutEsNsscProject extends ConfigurableStop{
  override val authorEmail: String = "yit"
  override val description: String = "Put data to mongodb"
  val inportList: List[String] = List(Port.DefaultPort.toString)
  val outportList: List[String] = List(Port.NonePort.toString)


  var index:String=_
  var indexTypes:String=_
  var port:String=_
  var ip:String=_

  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    val spark: SparkSession = pec.get[SparkSession]()
    val df: DataFrame = in.read()

   val project = new Project



    val ipAndPort=ip+":"+port


    var projectId=""
    var types=""
    var source=""
    var fundType=""
    var number=""
    var chineseName =""
    var englishName=""
    var organization=""
    var leadUnit =""
    var leadUnitDepartment=""
    var applicant=""
    var submitter=""
    var applicationFilePath=""
    var applyForFunds=""
    var approveTime=""
    var approveNumber=""
    var approveNoticePath=""
    var approveFunds=""
    var startTime=""
    var entTime=""
    var charge=""
    var coOrganization=""
    var parent=""
    var coOrganizationCount=""
    var isContinuous=""
    var formerProject=""
    var applyCode1 =""
    var applyCode2=""
    var departmentNo =""
    var projectNature=""
    var status=""
    var grantDescription=""
    var coAgreement=""
    var totalInAmount=""
    var isPayComplete=""
    var postDoctorCount=""
    var doctorCount=""
    var masterCount=""
    var middleCount=""
    var juniorCount=""
    var seniorCount=""
    var totalCount=""
    var changeAmount=""
    var chineseKeyword=""
    var englishKeyword=""
    var changeStatus=""
    var isGrantDelay=""
    var changeFileStatus=""
    var finalFileCode=""
    var name=""
    var lastUpdate=""
    var commonId=""
    var patent=""
    var chineseAbstract=""
    var englishAbstract=""
    var finalAbstract=""
    var subject=""



    var rewardNum=0
    var patentNum=0
    var monographNum=0
    var criterionNum=0
    var resultsTotal=0
    var total=0
    var journalPaperNum=0
    var cnJournalPaperNum=0
    var enJournalPaperNum=0
    var conferencePaperNum=0

    val unit: RDD[Project] = df.rdd.map(t => {
      projectId = t.getAs[String]("projectId")
      project.setProjectId(projectId)

      val year = t.getAs[Integer]("year")
      project.setYear(year)

      types = t.getAs[String]("type")
      project.setTypes(types)

      source = t.getAs[String]("source")
      project.setSource(source)

      fundType = t.getAs[String]("fundType")
      project.setFundType(fundType)

      number = t.getAs[String]("number")
      project.setNumber(number)

      chineseName = t.getAs[String]("chineseName")
      project.setChineseName(chineseName)

      englishName = t.getAs[String]("englishName")
      project.setEnglishName(englishName)

      organization = t.getAs[String]("organization")
      project.setOrganization(organization)

      leadUnit = t.getAs[String]("leadUnit")
      project.setLeadUnit(leadUnit)

      leadUnitDepartment = t.getAs[String]("leadUnitDepartment")
      project.setLeadUnitDepartment(leadUnitDepartment)

      applicant = t.getAs[String]("applicant")
      project.setApplicant(applicant)

      submitter = t.getAs[String]("submitter")
      project.setSubmitter(submitter)

      applicationFilePath = t.getAs[String]("applicationFilePath")
      project.setApplicationFilePath(applicationFilePath)

      applyForFunds = t.getAs[String]("applyForFunds")
      project.setApplyForFunds(applyForFunds)

      approveTime = t.getAs[String]("approveTime")
      project.setApproveTime(approveTime)

      approveNumber = t.getAs[String]("approveNumber")
      project.setApproveNumber(approveNumber)

      approveNoticePath = t.getAs[String]("approveNoticePath")
      project.setApproveNoticePath(approveNoticePath)

      approveFunds = t.getAs[String]("approveFunds")
      project.setApproveFunds(approveFunds)

      startTime = t.getAs[String]("startTime")
      project.setStartTime(startTime)

      entTime = t.getAs[String]("entTime")
      project.setEntTime(entTime)

      charge = t.getAs[String]("charge")
      project.setCharge(charge)

      coOrganization = t.getAs[String]("coOrganization")
      project.setCoOrganization(coOrganization)

      parent = t.getAs[String]("parent")
      project.setParent(parent)

      coOrganizationCount = t.getAs[String]("coOrganizationCount")
      project.setCoOrganizationCount(coOrganizationCount)

      isContinuous = t.getAs[String]("isContinuous")
      project.setIsContinuous(isContinuous)

      formerProject = t.getAs[String]("formerProject")
      project.setFormerProject(formerProject)

      applyCode1 = t.getAs[String]("applyCode1")
      project.setApplyCode1(applyCode1)

      applyCode2 = t.getAs[String]("applyCode2")
      project.setApplyCode2(applyCode2)

      departmentNo = t.getAs[String]("departmentNo")
      project.setDepartmentNo(departmentNo)

      projectNature = t.getAs[String]("projectNature")
      project.setProjectNature(projectNature)

      status = t.getAs[String]("status")
      project.setStatus(status)

      grantDescription = t.getAs[String]("grantDescription")
      project.setGrantDescription(grantDescription)


      coAgreement = t.getAs[String]("coAgreement")
      project.setCoAgreement(coAgreement)

      totalInAmount = t.getAs[String]("totalInAmount")
      project.setTotalInAmount(totalInAmount)

      isPayComplete = t.getAs[String]("isPayComplete")
      project.setIsPayComplete(isPayComplete)

      postDoctorCount = t.getAs[String]("postDoctorCount")
      project.setPostDoctorCount(postDoctorCount)

      doctorCount = t.getAs[String]("doctorCount")
      project.setDoctorCount(doctorCount)

      masterCount = t.getAs[String]("masterCount")
      project.setMasterCount(masterCount)


      middleCount = t.getAs[String]("middleCount")
      project.setMiddleCount(middleCount)

      juniorCount = t.getAs[String]("juniorCount")
      project.setJuniorCount(juniorCount)

      seniorCount = t.getAs[String]("seniorCount")
      project.setSeniorCount(seniorCount)

      totalCount = t.getAs[String]("totalCount")
      project.setTotalCount(totalCount)

      changeAmount = t.getAs[String]("changeAmount")
      project.setChangeAmount(changeAmount)

      chineseKeyword = t.getAs[String]("chineseKeyword")
      project.setChineseKeyword(chineseKeyword)

      englishKeyword = t.getAs[String]("englishKeyword")
      project.setEnglishKeyword(englishKeyword)

      changeStatus = t.getAs[String]("changeStatus")
      project.setChangeStatus(changeStatus)

      isGrantDelay = t.getAs[String]("isGrantDelay")
      project.setIsGrantDelay(isGrantDelay)

      changeFileStatus = t.getAs[String]("changeFileStatus")
      project.setChangeFileStatus(changeFileStatus)

      finalFileCode = t.getAs[String]("finalFileCode")
      project.setFinalFileCode(finalFileCode)

      name = t.getAs[String]("name")
      project.setName(name)

      lastUpdate = t.getAs[String]("lastUpdate")
      project.setLastUpdate(lastUpdate)

      commonId = t.getAs[String]("commonId")
      project.setCommonId(commonId)

      patent = t.getAs[String]("patent")
      project.setParent(patent)

      chineseAbstract = t.getAs[String]("chineseAbstract")
      project.setChineseAbstract(chineseAbstract)

      englishAbstract = t.getAs[String]("englishAbstract")
      project.setEnglishAbstract(englishAbstract)

      finalAbstract = t.getAs[String]("finalAbstract")
      project.setFinalAbstract(finalAbstract)



      rewardNum = t.getAs[Integer]("rewardNum")
      project.setRewardNum(rewardNum)


      patentNum = t.getAs[Integer]("patentNum")
      project.setPatentNum(patentNum)



      monographNum = t.getAs[Integer]("monographNum")
      project.setMonographNum(monographNum)



      resultsTotal = t.getAs[Integer]("resultsTotal")
      project.setResultsTotal(resultsTotal)





      total = t.getAs[Integer]("total")
      project.setTotal(total)

      journalPaperNum = t.getAs[Integer]("journalPaperNum")
      project.setJournalPaperNum(journalPaperNum)

      cnJournalPaperNum = t.getAs[Integer]("cnJournalPaperNum")
      project.setCnJournalPaperNum(cnJournalPaperNum)

      enJournalPaperNum = t.getAs[Integer]("enJournalPaperNum")
      project.setEnJournalPaperNum(enJournalPaperNum)

      conferencePaperNum = t.getAs[Integer]("conferencePaperNum")
      project.setConferencePaperNum(conferencePaperNum)


      subject = t.getAs[String]("subject")
      project.setSubject(subject)
      project
    })


    var map :Map[String,Any]=null
    unit.map(t=>{
      map = Map(
        if(t.getProjectId!="" && t.getProjectId!="null"){
          "projectId"->t.getProjectId
        }else{
          "projectId"->null
        },

        if(!"null".equals(t.getYear)&& t.getYear!=null &&t.getYear!="" && t.getYear!="null"){
          "year"->t.getYear
        }else{
          "year"->null
        },

        if(t.getTypes!="" && t.getTypes!="null"){
          "type"->t.getTypes
        }else{
          "type"->null
        },

        if(t.getSource!="" && t.getSource!="null"){
          "source"->t.getSource
        }else{
          "source"->null
        },

        if(t.getFundType!="" && t.getFundType!="null"){
          "fundType"->t.getFundType
        }else{
          "fundType"->null
        },

        if(t.getNumber!="" && t.getNumber!="null"){
          "number"->t.getNumber
        }else{
          "number"->null
        },


        if(t.getChineseName!="" && t.getChineseName!="null"){
          "chineseName"->t.getChineseName
        }else{
          "chineseName"->null
        },

        if(t.getEnglishName!="" && t.getEnglishName!="null"){
          "englishName"->t.getEnglishName
        }else{
          "englishName"->null
        },

        if(t.getOrganization!="" && t.getOrganization!="null"){
          "organization"->JSON.parseArray(t.getOrganization,classOf[OrganizationNameAndId])
        }else{
          "organization"->null
        },

        if(t.getLeadUnit!="" && t.getLeadUnit!="null"){
          "leadUnit"->t.getLeadUnit
        }else{
          "leadUnit"->null
        },

        if(t.getLeadUnitDepartment!="" && t.getLeadUnitDepartment!="null"){
          "leadUnitDepartment"->t.getLeadUnitDepartment
        }else{
          "leadUnitDepartment"->null
        },

        if(t.getApplicant!="" && t.getApplicant!="null"){
          "applicant"->t.getApplicant
        }else{
          "applicant"->null
        },


        if(t.getSubmitter!="" && t.getSubmitter!="null"){
          "submitter"->t.getSubmitter
        }else{
          "submitter"->null
        },


        if(t.getApplicationFilePath!="" && t.getApplicationFilePath!="null"){
          "applicationFilePath"->t.getApplicationFilePath
        }else{
          "applicationFilePath"->null
        },


        if(t.getApplyForFunds!="" && t.getApplyForFunds!="null"){
          "applyForFunds"->t.getApplyForFunds
        }else{
          "applyForFunds"->null
        },


        if(t.getApproveTime!="" && t.getApproveTime!="null"){
          "approveTime"->t.getApproveTime
        }else{
          "approveTime"->null
        },

        if(t.getApproveNumber!="" && t.getApproveNumber!="null"){
          "approveNumber"->t.getApproveNumber
        }else{
          "approveNumber"->null
        },

        if(t.getApproveNoticePath!="" && t.getApproveNoticePath!="null"){
          "approveNoticePath"->t.getApproveNoticePath
        }else{
          "approveNoticePath"->null
        },

        if(t.getApproveFunds!="" && t.getApproveFunds!="null"){
          "approveFunds"->t.getApproveFunds
        }else{
          "approveFunds"->null
        },

        if(t.getStartTime!="" && t.getStartTime!="null"){
          "startTime"->new SimpleDateFormat("yyyy-MM-dd").parse(t.getStartTime)
        }else{
          "startTime"->null
        },

        if(t.getEntTime!="" && t.getEntTime!="null"){
          "entTime"->new SimpleDateFormat("yyyy-MM-dd").parse(t.getEntTime)
        }else{
          "entTime"->null
        },

        if(t.getCharge!="" && t.getCharge!="null"){
          "charge"->JSON.parseObject(t.getCharge,classOf[Charge])
        }else{
          "charge"->null
        },

        if(t.getCoOrganization!="" && t.getCoOrganization!="null"){
          "coOrganization"->t.getCoOrganization
        }else{
          "coOrganization"->null
        },

        if(t.getParent!="" && t.getParent!="null"){
          "parent"->t.getParent
        }else{
          "parent"->null
        },

        if(t.getCoOrganizationCount!="" && t.getCoOrganizationCount!="null"){
          "coOrganizationCount"->t.getCoOrganizationCount
        }else{
          "coOrganizationCount"->null
        },

        if(t.getIsContinuous!="" && t.getIsContinuous!="null"){
          "isContinuous"->t.getIsContinuous
        }else{
          "isContinuous"->null
        },

        if(t.getFormerProject!="" && t.getFormerProject!="null"){
          "formerProject"->t.getFormerProject
        }else{
          "formerProject"->null
        },

        if(t.getApplyCode1!="" && t.getApplyCode1!="null"){
          "applyCode1"->t.getApplyCode1
        }else{
          "applyCode1"->null
        },

        if(t.getApplyCode2!="" && t.getApplyCode2!="null"){
          "applyCode2"->t.getApplyCode2
        }else{
          "applyCode2"->null
        },

        if(t.getDepartmentNo!="" && t.getDepartmentNo!="null"){
          "departmentNo"->t.getDepartmentNo
        }else{
          "departmentNo"->null
        },

        if(t.getProjectNature!="" && t.getProjectNature!="null"){
          "projectNature"->t.getProjectNature
        }else{
          "projectNature"->null
        },

        if(t.getStatus!="" && t.getStatus!="null"){
          "status"->t.getStatus
        }else{
          "status"->null
        },

        if(t.getGrantDescription!="" && t.getGrantDescription!="null"){
          "grantDescription"->t.getGrantDescription
        }else{
          "grantDescription"->null
        },


        if(t.getCoAgreement!="" && t.getCoAgreement!="null"){
          "coAgreement"->t.getCoAgreement
        }else{
          "coAgreement"->null
        },

        if(t.getTotalInAmount!="" && t.getTotalInAmount!="null"){
          "totalInAmount"->t.getTotalInAmount
        }else{
          "totalInAmount"->null
        },

        if(t.getTypes!="" && t.getTypes!="null"){
          "isPayComplete"->t.getIsPayComplete
        }else{
          "isPayComplete"->null
        },

        if(t.getPostDoctorCount!="" && t.getPostDoctorCount!="null"){
          "postDoctorCount"->t.getPostDoctorCount
        }else{
          "postDoctorCount"->null
        },

        if(t.getDoctorCount!="" && t.getDoctorCount!="null"){
          "doctorCount"->t.getDoctorCount
        }else{
          "doctorCount"->null
        },

        if(t.getMasterCount!="" && t.getMasterCount!="null"){
          "masterCount"->t.getMasterCount
        }else{
          "masterCount"->null
        },

        if(t.getMiddleCount!="" && t.getMiddleCount!="null"){
          "middleCount"->t.getMiddleCount
        }else{
          "middleCount"->null
        },

        if(t.getJuniorCount!="" && t.getJuniorCount!="null"){
          "juniorCount"->t.getJuniorCount
        }else{
          "juniorCount"->null
        },

        if(t.getSeniorCount!="" && t.getSeniorCount!="null"){
          "seniorCount"->t.getSeniorCount
        }else{
          "seniorCount"->null
        },

        if(t.getTotalCount!="" && t.getTotalCount!="null"){
          "totalCount"->t.getTotalCount
        }else{
          "totalCount"->null
        },

        if(t.getChangeAmount!="" && t.getChangeAmount!="null"){
          "changeAmount"->t.getChangeAmount
        }else{
          "changeAmount"->null
        },

        if(!"null".equals(t.getChineseKeyword) && t.getChineseKeyword!=null && t.getChineseKeyword!="null"){
          "chineseKeyword"->t.getChineseKeyword.split(",")
        }else{
          "chineseKeyword"->null
        },

        if(!"null".equals(t.getEnglishKeyword) && t.getEnglishKeyword!=null && t.getEnglishKeyword!="null"){
          "englishKeyword"->t.getEnglishKeyword.split(",")
        }else{
          "englishKeyword"->null
        },

        if(t.getChangeStatus!="" && t.getChangeStatus!="null"){
          "changeStatus"->t.getChangeStatus
        }else{
          "changeStatus"->null
        },

        if(t.getIsGrantDelay!="" && t.getIsGrantDelay!="null"){
          "isGrantDelay"->t.getIsGrantDelay
        }else{
          "isGrantDelay"->null
        },

        if(t.getChangeFileStatus!="" && t.getChangeFileStatus!="null"){
          "changeFileStatus"->t.getChangeFileStatus
        }else{
          "changeFileStatus"->null
        },

        if(t.getFinalFileCode!="" && t.getFinalFileCode!="null"){
          "finalFileCode"->t.getFinalFileCode
        }else{
          "finalFileCode"->null
        },

        if(t.getName!="" && t.getName!="null"){
          "name"->t.getName
        }else{
          "name"->null
        },

        if(t.getLastUpdate!="" && t.getLastUpdate!="null"){
          "lastUpdate"->t.getLastUpdate
        }else{
          "lastUpdate"->null
        },

        if(t.getCommonId!="" && t.getCommonId!="null"){
          "commonId"->t.getCommonId
        }else{
          "commonId"->null
        },

        if(t.getPatent!="" && t.getPatent!="null"){
          "patent"->t.getPatent
        }else{
          "patent"->null
        },

        if(t.getChineseAbstract!="" && t.getChineseAbstract!="null"){
          "chineseAbstract"->t.getChineseAbstract
        }else{
          "chineseAbstract"->null
        },

        if(t.getEnglishAbstract!="" && t.getEnglishAbstract!="null"){
          "englishAbstract"->t.getEnglishAbstract
        }else{
        "englishAbstract"->null
      },

        if(t.getFinalAbstract!="" && t.getFinalAbstract!="null"){
        "finalAbstract"->t.getFinalAbstract
      }else{
        "finalAbstract"->null
      },

        "rewardNum"->t.getRewardNum,
        "patentNum"->t.getPatentNum,
        "monographNum"->t.getMonographNum,
        "criterionNum"->t.getCriterionNum,
        "resultsTotal"->t.getResultsTotal,
        "total"->t.getTotal,
        "journalPaperNum"->t.getJournalPaperNum,
        "cnJournalPaperNum"->t.getCnJournalPaperNum,
        "enJournalPaperNum"->t.getEnJournalPaperNum,
        "conferencePaperNum"->t.getConferencePaperNum,


      if(t.getSubject!="" && t.getSubject!="null"){
        "subject"->JSON.parseArray(t.getSubject,classOf[Rank])
      }else{
        "subject"->null
      }

      )
      map
    }).saveToEs(index+"/"+indexTypes,Map("es.mapping.id"->"projectId",
      "es.index.auto.create"->"true",
      "es.nodes"->ipAndPort,
      "es.nodes.wan.only"->"true",
      "es.http.timeout"->"100m"
    ))
  }

  override def setProperties(map: Map[String, Any]): Unit = {
    index = MapUtil.get(map,"index").asInstanceOf[String]
    indexTypes = MapUtil.get(map,"indexTypes").asInstanceOf[String]
    ip = MapUtil.get(map,"ip").asInstanceOf[String]
    port = MapUtil.get(map,"port").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] = {
    var descriptor : List[PropertyDescriptor] = List()

    val index = new PropertyDescriptor().name("index").displayName("index").description("es index").defaultValue("").required(true)
    val indexTypes = new PropertyDescriptor().name("indexTypes").displayName("indexTypes").description("es type").defaultValue("").required(true)
    val ip = new PropertyDescriptor().name("ip").displayName("ip").description("es ip address").defaultValue("").required(true)
    val port = new PropertyDescriptor().name("port").displayName("port").description("es port").defaultValue("").required(true)
    descriptor = index :: descriptor
    descriptor = ip :: descriptor
    descriptor = port :: descriptor
    descriptor = indexTypes :: descriptor

    descriptor
  }

  override def getIcon(): Array[Byte] =  {
    ImageUtil.getImage("icon/nssc/put-mongo-nssc.png")
  }

  override def getGroup(): List[String] = {
    List("")
  }

  override def initialize(ctx: ProcessContext): Unit = { }

}
