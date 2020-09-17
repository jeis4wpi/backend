import { QueryInterface, DataTypes } from 'sequelize';

export default {
  up: async (queryInterface: QueryInterface): Promise<void> => {
    await queryInterface.createTable('student_grade_override', {
      id: {
        field: 'student_grade_override_id',
        type: DataTypes.INTEGER,
        autoIncrement: true,
        primaryKey: true,
      },
      studentGradeId: {
        field: 'student_grade_id',
        type: DataTypes.INTEGER,
        allowNull: false,
        references: {
          model: 'student_grade',
          key: 'student_grade_id',
        },
      },
      initiatingUserId: {
        field: 'initiating_user_id',
        type: DataTypes.INTEGER,
        allowNull: false,
        references: {
          model: 'users',
          key: 'user_id',
        }
      },
      newValue: {
        field: 'student_grade_override_new_value',
        type: DataTypes.INTEGER,
        allowNull: false
      },
      updatedAt: {
        field: 'updated_at',
        type: DataTypes.DATE,
        allowNull: false,
      },
      createdAt: {
        field: 'created_at',
        type: DataTypes.DATE,
        allowNull: false,
      },
    });

    // This is a hack to add the associations later to avoid cyclic dependencies
    /* eslint-disable @typescript-eslint/no-use-before-define */
  },
  down: async (queryInterface: QueryInterface): Promise<void> => {
    await queryInterface.dropTable('student_grade_override');
  }
};