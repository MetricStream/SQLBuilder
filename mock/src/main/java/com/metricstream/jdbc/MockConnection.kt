package com.metricstream.jdbc

import java.sql.Blob
import java.sql.CallableStatement
import java.sql.Clob
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.NClob
import java.sql.PreparedStatement
import java.sql.SQLWarning
import java.sql.SQLXML
import java.sql.Savepoint
import java.sql.Statement
import java.sql.Struct
import java.util.Properties
import java.util.concurrent.Executor

class MockConnection : Connection {
    override fun <T : Any?> unwrap(p0: Class<T>?): T {
        error("Should never be called")
    }

    override fun isWrapperFor(p0: Class<*>?): Boolean {
        error("Should never be called")
    }

    override fun close() {
    }

    override fun createStatement(): Statement {
        error("Should never be called")
    }

    override fun createStatement(p0: Int, p1: Int): Statement {
        error("Should never be called")
    }

    override fun createStatement(p0: Int, p1: Int, p2: Int): Statement {
        error("Should never be called")
    }

    override fun prepareStatement(p0: String?): PreparedStatement {
        error("Should never be called")
    }

    override fun prepareStatement(p0: String?, p1: Int, p2: Int): PreparedStatement {
        error("Should never be called")
    }

    override fun prepareStatement(p0: String?, p1: Int, p2: Int, p3: Int): PreparedStatement {
        error("Should never be called")
    }

    override fun prepareStatement(p0: String?, p1: Int): PreparedStatement {
        error("Should never be called")
    }

    override fun prepareStatement(p0: String?, p1: IntArray?): PreparedStatement {
        error("Should never be called")
    }

    override fun prepareStatement(p0: String?, p1: Array<out String>?): PreparedStatement {
        error("Should never be called")
    }

    override fun prepareCall(p0: String?): CallableStatement {
        error("Should never be called")
    }

    override fun prepareCall(p0: String?, p1: Int, p2: Int): CallableStatement {
        error("Should never be called")
    }

    override fun prepareCall(p0: String?, p1: Int, p2: Int, p3: Int): CallableStatement {
        error("Should never be called")
    }

    override fun nativeSQL(p0: String?): String {
        error("Should never be called")
    }

    override fun setAutoCommit(p0: Boolean) {
    }

    override fun getAutoCommit(): Boolean {
        error("Should never be called")
    }

    override fun commit() {
    }

    override fun rollback() {
    }

    override fun rollback(p0: Savepoint?) {
    }

    override fun isClosed(): Boolean {
        error("Should never be called")
    }

    override fun getMetaData(): DatabaseMetaData {
        error("Should never be called")
    }

    override fun setReadOnly(p0: Boolean) {
        error("Should never be called")
    }

    override fun isReadOnly(): Boolean {
        error("Should never be called")
    }

    override fun setCatalog(p0: String?) {
        error("Should never be called")
    }

    override fun getCatalog(): String {
        error("Should never be called")
    }

    override fun setTransactionIsolation(p0: Int) {
        error("Should never be called")
    }

    override fun getTransactionIsolation(): Int {
        error("Should never be called")
    }

    override fun getWarnings(): SQLWarning {
        error("Should never be called")
    }

    override fun clearWarnings() {
        error("Should never be called")
    }

    override fun getTypeMap(): MutableMap<String, Class<*>> {
        error("Should never be called")
    }

    override fun setTypeMap(p0: MutableMap<String, Class<*>>?) {
        error("Should never be called")
    }

    override fun setHoldability(p0: Int) {
        error("Should never be called")
    }

    override fun getHoldability(): Int {
        error("Should never be called")
    }

    override fun setSavepoint(): Savepoint {
        error("Should never be called")
    }

    override fun setSavepoint(p0: String?): Savepoint {
        error("Should never be called")
    }

    override fun releaseSavepoint(p0: Savepoint?) {
        error("Should never be called")
    }

    override fun createClob(): Clob {
        error("Should never be called")
    }

    override fun createBlob(): Blob {
        error("Should never be called")
    }

    override fun createNClob(): NClob {
        error("Should never be called")
    }

    override fun createSQLXML(): SQLXML {
        error("Should never be called")
    }

    override fun isValid(p0: Int): Boolean {
        error("Should never be called")
    }

    override fun setClientInfo(p0: String?, p1: String?) {
        error("Should never be called")
    }

    override fun setClientInfo(p0: Properties?) {
        error("Should never be called")
    }

    override fun getClientInfo(p0: String?): String {
        error("Should never be called")
    }

    override fun getClientInfo(): Properties {
        error("Should never be called")
    }

    override fun createArrayOf(p0: String?, p1: Array<out Any>?): java.sql.Array {
        error("Should never be called")
    }

    override fun createStruct(p0: String?, p1: Array<out Any>?): Struct {
        error("Should never be called")
    }

    override fun setSchema(p0: String?) {
        error("Should never be called")
    }

    override fun getSchema(): String {
        error("Should never be called")
    }

    override fun abort(p0: Executor?) {
        error("Should never be called")
    }

    override fun setNetworkTimeout(p0: Executor?, p1: Int) {
        error("Should never be called")
    }

    override fun getNetworkTimeout(): Int {
        error("Should never be called")
    }
}
