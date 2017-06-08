//My method
	@Override
	public void cleanUpTableData1(String mainTable, String backupTable,
			String joinConditionMove, String joinConditionDel, String condition) throws DSPException {
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		String transactionId = null;
		ResultSet rs = null;
		boolean result = false;

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(businessDataStore);
			transactionId = businessDataStore.beginTransaction();
			
			/** Insert the records from mainTable to its backupTable for all records satisfying condition **/
			String sql = "INSERT INTO " + backupTable + " SELECT * FROM " + mainTable + " " + joinConditionMove  + " where "
							+ condition;
			
			ps = businessDataStore.createPreparedStatement(sql);
		
			int resultInsert = ps.executeUpdate();
			if(resultInsert > 0) {
				System.out.println("CleanUpDSPImpl :: cleanUpTable :: moved " + resultInsert + " records from " + mainTable + " to " + backupTable);
			}
			ps.close();
			
			/** Delete the records from mainTable  which are moved to backupTable **/
			sql = "DELETE FROM " + mainTable + " where " + joinConditionDel + " where "
					+ condition;
			
			ps = businessDataStore.createPreparedStatement(sql);
			int resultUpdate = ps.executeUpdate();
			businessDataStore.commitTransaction(transactionId);
			System.out.println("CleanUpDSPImpl :: cleanUpTable :: Deleted " + resultUpdate + "records from " + mainTable);
			result = true;
		} catch (SQLException se) {
			se.printStackTrace();
			throw new DSPException("Error while inserting into db", se);
		} finally {
			if (!result) {
				businessDataStore.rollbackTransaction(transactionId);
			}
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				rs = null;
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				ps = null;
			}
			DataStoreContext.clearDataStoreContext(dataStoreOwnerKey);
		}
	}