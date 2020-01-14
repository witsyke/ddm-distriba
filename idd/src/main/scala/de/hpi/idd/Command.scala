package de.hpi.idd

import com.beust.jcommander.Parameter

class Command {


    @Parameter(names = Array("-p", "--path"))
    var input_path: String = _

  @Parameter(names = Array("-c", "--cores"))
  var input_cores: Int = _

  @Parameter(names = Array("-pt", "--partitions"))
  var input_partitions: Int = _


}
