/*
 * Copyright (c) 2014 eBay, Inc.
 * All rights reserved.
 *
 * Contributors:
 * asucharitakul
 */
package org.squbs.unicomplex

import scala.collection.JavaConversions._
import scala.util.Try
import com.typesafe.config.Config


object ConfigUtil {

  implicit class RichConfig(val underlying: Config) extends AnyVal {

    def getOptionalInt(path: String): Option[Int] =
      Try(underlying.getInt(path)) toOption

    def getOptionalBoolean(path: String): Option[Boolean] =
      Try(underlying.getBoolean(path)) toOption

    def getOptionalString(path: String): Option[String] =
      Try(underlying.getString(path)) toOption

    def getOptionalConfig(path: String): Option[Config] =
      Try(underlying.getConfig(path)) toOption

    def getOptionalStringList(path: String): Option[Seq[String]] =
      Try(underlying.getStringList(path).toSeq) toOption

    def getOptionalConfigList(path: String): Option[Seq[Config]] =
      Try(underlying.getConfigList(path).toSeq) toOption
  }
}