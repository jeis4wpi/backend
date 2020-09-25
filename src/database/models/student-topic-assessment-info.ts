import { Model, DataTypes } from 'sequelize';
import appSequelize from '../app-sequelize';
import CourseTopicContent from './course-topic-content';

interface StudentTopicAssessmentInfoInterface {
    id: number;
    courseTopicContentId: number;
    userId: number;
    startTime: Date;
    endTime: Date;
    active: boolean;
}

export default class StudentTopicAssessmentInfo extends Model implements StudentTopicAssessmentInfoInterface {
    public id!: number;
    public courseTopicContentId!: number;
    public userId!: number;
    public startTime!: Date;
    public endTime!: Date;
    public active!: boolean;


    // public getCurriculumTopicContent!: BelongsToGetAssociationMixin<CurriculumTopicContent>;

    // public readonly curriculumTopicContent!: CurriculumTopicContent;

    // timestamps!
    public readonly createdAt!: Date;
    public readonly updatedAt!: Date;

    static constraints = {
    }

    static createAssociations(): void {
        // This is a hack to add the associations later to avoid cyclic dependencies
        /* eslint-disable @typescript-eslint/no-use-before-define */
        StudentTopicAssessmentInfo.belongsTo(CourseTopicContent, {
            foreignKey: 'courseTopicContentId',
            targetKey: 'id',
            as: 'studentGrade'
        });

        StudentTopicAssessmentInfo.belongsTo(User, {
            foreignKey: 'userId',
            targetKey: 'id',
            as: 'user'
        });
        // CourseTopicContent.hasMany(CourseWWTopicQuestion, {
        //     foreignKey: 'courseTopicContentId',
        //     sourceKey: 'id',
        //     as: 'questions'
        // });
        /* eslint-enable @typescript-eslint/no-use-before-define */
    }
}

StudentTopicAssessmentInfo.init({
    id: {
        field: 'student_topic_assessment_info_id',
        type: DataTypes.INTEGER,
        autoIncrement: true,
        primaryKey: true,
    },
    courseTopicContentId: {
        field: 'course_topic_content_id',
        type: DataTypes.INTEGER,
        allowNull: false
    },
    userId: {
        field: 'user_id',
        type: DataTypes.INTEGER,
        allowNull: false
    },
    startDate: {
        field: 'student_topic_assessment_info_start_date',
        type: DataTypes.DATE,
        allowNull: false,
        defaultValue: appSequelize.literal('NOW()')
    },
    endDate: {
        field: 'student_topic_assessment_info_end_date',
        type: DataTypes.DATE,
        allowNull: false,
        defaultValue: appSequelize.literal('NOW()')
    },
    active: {
        field: 'student_topic_assessment_info_active',
        type: DataTypes.BOOLEAN,
        allowNull: false,
        defaultValue: true
    },
}, {
    tableName: 'student_topic_assessment_info',
    sequelize: appSequelize, // this bit is important
});

import User from './user';