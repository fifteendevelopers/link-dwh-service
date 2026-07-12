<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        Schema::create('Fact_Grant_Recipient_Renewal', function (Blueprint $table) {
            $table->bigIncrements('Renewal_Key');
            $table->unsignedBigInteger('Recipient_Key')->index();
            $table->unsignedBigInteger('Source_Renewal_Id')->unique();
            $table->unsignedBigInteger('Source_System_Key');

            $table->tinyInteger('Status');
            $table->smallInteger('Renewal_Year');
            $table->date('Date_Due');
            $table->date('Date_Completed')->nullable();
            $table->date('Date_Approved')->nullable();

            $table->tinyInteger('Flag_Reminder_Warning')->default(0);
            $table->tinyInteger('Flag_Overdue_Warning')->default(0);

            $table->string('Q_Organisation_Category')->nullable();
            $table->tinyInteger('Q_Confirm_Details')->default(0);
            $table->json('Q_Contracted_Deliveries')->nullable();
            $table->tinyInteger('Q_Confirm_Guidelines_Commitment')->default(0);
            $table->tinyInteger('Q_Confirm_Risk_Assessment')->default(0);
            $table->tinyInteger('Q_Confirm_Policy_Review')->default(0);
            $table->tinyInteger('Q_Confirm_Valid_Insurance')->default(0);
            $table->json('Q_Documents')->nullable();
            $table->tinyInteger('Q_Confirm_Access')->default(0);
            $table->tinyInteger('Q_Confirm_Associated_TP')->default(0);
            $table->tinyInteger('Q_Confirm_Assurance_One')->default(0);
            $table->tinyInteger('Q_Confirm_Assurance_Two')->default(0);
            $table->tinyInteger('Q_Confirm_Assurance_Three')->default(0);
            $table->tinyInteger('Q_Confirm_Incidents')->default(0);

            $table->text('Q_Stage_1_Complaints')->nullable();
            $table->text('Q_Serious_Complaints')->nullable();
            $table->tinyInteger('Q_Confirm_Final')->default(0);
            $table->text('Q_Name')->nullable();
            $table->tinyInteger('Q_GR_Is_Also_TP')->default(0);
            $table->text('Q_Safeguarding')->nullable();

            $table->json('Users_With_Access_To_GR')->nullable();
            $table->json('Associated_TPs')->nullable();
            $table->tinyInteger('Q_Tfl_Funded')->nullable();
            $table->json('Contact_Details_Confirmed')->nullable();

            $table->timestamp('Source_Created_At')->nullable();
            $table->timestamp('Source_Updated_At')->nullable();
            $table->timestamps(); // DWH record tracking
        });


        Schema::create('Fact_Instructor_Renewal', function (Blueprint $table) {
            $table->bigIncrements('Renewal_Key');
            $table->unsignedBigInteger('Instructor_Key')->index();
            $table->unsignedBigInteger('Source_Renewal_Id')->unique();
            $table->unsignedBigInteger('Source_System_Key');

            $table->tinyInteger('Status');
            $table->smallInteger('Renewal_Year');
            $table->date('Date_Due');
            $table->date('Date_Completed')->nullable();
            $table->date('Date_Approved')->nullable();

            $table->tinyInteger('Flag_Reminder_Warning')->default(0);
            $table->tinyInteger('Flag_Overdue_Warning')->default(0);

            $table->tinyInteger('Q_Confirm_Registration_Details')->default(0);
            $table->tinyInteger('Q_Confirm_Status')->nullable()->default(0);
            $table->tinyInteger('Q_Confirm_Delivering_Bikeability')->nullable();
            $table->tinyInteger('Q_Confirm_Observed_By_IQA_Lead')->nullable();
            $table->tinyInteger('Q_Confirm_Completed_CPD')->nullable();
            $table->tinyInteger('Q_Confirm_Code_of_Practice')->nullable();
            $table->tinyInteger('Q_Confirm_Delivering_Other')->nullable()->default(0);
            $table->tinyInteger('Q_Confirm_Contact_Prefs')->default(0);
            $table->tinyInteger('Q_Confirm_Fee')->nullable()->default(0);

            $table->json('Q_Delivery_Examples')->nullable();
            $table->tinyInteger('Q_Delivery_Amount_Level_1')->nullable()->default(0);
            $table->tinyInteger('Q_Delivery_Amount_Level_1_2')->nullable()->default(0);
            $table->tinyInteger('Q_Delivery_Amount_Level_2')->nullable()->default(0);
            $table->tinyInteger('Q_Delivery_Amount_Level_3')->nullable()->default(0);
            $table->tinyInteger('Q_Delivery_Amount_Plus')->nullable()->default(0);

            $table->json('Q_Professional_Development')->nullable();
            $table->json('Q_Strengths')->nullable();

            $table->timestamp('Source_Created_At')->nullable();
            $table->timestamp('Source_Updated_At')->nullable();
            $table->timestamps();
        });

        Schema::create('Fact_Training_Provider_Renewal', function (Blueprint $table) {
            $table->bigIncrements('Renewal_Key');
            $table->unsignedBigInteger('Provider_Key')->index()->comment('FK to Dim_Training_Provider');
            $table->unsignedBigInteger('Source_Renewal_Id')->unique();
            $table->unsignedBigInteger('Source_System_Key');

            $table->tinyInteger('Status');
            $table->tinyInteger('Approved')->nullable();
            $table->smallInteger('Renewal_Year');
            $table->date('Date_Due');
            $table->date('Date_Completed')->nullable();
            $table->date('Date_Approved')->nullable();

            $table->tinyInteger('Flag_Reminder_Warning')->default(0);
            $table->tinyInteger('Flag_Overdue_Warning')->default(0);

            $table->string('Q_Organisation_Category')->nullable();
            $table->tinyInteger('Q_Confirm_Details')->default(0);
            $table->json('Q_Contracted_Deliveries')->nullable();
            $table->json('Q_Delivered_Places')->nullable();
            $table->json('Q_Instructor_Types_Breakdown')->nullable();

            $table->tinyInteger('Q_Confirm_Instructor_DbsCert')->default(0);
            $table->tinyInteger('Q_Confirm_Instructor_SafetyPolicies')->default(0);

            $table->string('Q_Delivery_Amount_Level_1')->nullable();
            $table->string('Q_Delivery_Amount_Level_1_2')->nullable();
            $table->string('Q_Delivery_Amount_Level_2')->nullable();
            $table->string('Q_Delivery_Amount_Level_3')->nullable();
            $table->string('Q_Delivery_Amount_Plus_Balance')->nullable();
            $table->string('Q_Delivery_Amount_Plus_Bus')->nullable();
            $table->string('Q_Delivery_Amount_Plus_Fix')->nullable();
            $table->string('Q_Delivery_Amount_Plus_Learn')->nullable();
            $table->string('Q_Delivery_Amount_Plus_On_Show')->nullable();
            $table->string('Q_Delivery_Amount_Plus_Parents')->nullable();
            $table->string('Q_Delivery_Amount_Plus_Promotion')->nullable();
            $table->string('Q_Delivery_Amount_Plus_Recycled')->nullable();
            $table->string('Q_Delivery_Amount_Plus_Ride')->nullable();
            $table->string('Q_Delivery_Amount_Plus_Transition')->nullable();

            $table->json('Q_Delivery_Model')->nullable();
            $table->tinyInteger('Q_Confirm_Guidelines_Commitment')->default(0);
            $table->tinyInteger('Q_Confirm_Risk_Assessment')->default(0);
            $table->tinyInteger('Q_Confirm_Policy_Review')->default(0);
            $table->tinyInteger('Q_Confirm_Valid_Insurance')->default(0);
            $table->json('Q_Documents')->nullable();
            $table->tinyInteger('Q_Confirm_Iqa_Plan')->default(0);
            $table->json('Q_Strengths')->nullable();

            $table->text('Q_Priority_1')->nullable();
            $table->text('Q_Action_Plan_1')->nullable();
            $table->text('Q_Priority_2')->nullable();
            $table->text('Q_Action_Plan_2')->nullable();
            $table->text('Q_Priority_3')->nullable();
            $table->text('Q_Action_Plan_3')->nullable();

            $table->tinyInteger('Q_Associated_Instructors_Two')->default(0);
            $table->integer('Q_Children')->default(0);
            $table->integer('Q_Adults')->default(0);
            $table->integer('Q_Families')->default(0);
            $table->tinyInteger('Q_Confirm_Incidents')->default(0);
            $table->integer('Q_Stage_1_Complaints')->default(0);
            $table->integer('Q_Serious_Complaints')->default(0);

            $table->tinyInteger('Q_Confirm_Assurance_One')->default(0);
            $table->tinyInteger('Q_Confirm_Assurance_Two')->default(0);
            $table->tinyInteger('Q_Confirm_Assurance_Three')->default(0);
            $table->json('Q_Improvements')->nullable();

            $table->tinyInteger('Q_Confirm_Final_One')->default(0);
            $table->tinyInteger('Q_Confirm_Final_Two')->default(0);
            $table->tinyInteger('Q_Confirm_Final_Three')->default(0);
            $table->tinyInteger('Q_Associated_Instructors')->default(0);
            $table->tinyInteger('Q_Confirm_Access')->default(0);

            $table->string('Q_Safeguarding_Lead')->nullable();
            $table->string('Q_Health_And_Safety_Lead')->nullable();
            $table->string('Q_Iqa_Lead')->nullable();
            $table->string('Q_Iqa_Organisation')->nullable();
            $table->string('Q_Name')->nullable();
            $table->tinyInteger('Q_Associated_Instructors_Three')->nullable()->default(0);
            $table->text('Q_Safeguarding')->nullable();

            $table->json('Users_With_Access_To_TP')->nullable();
            $table->json('Associated_Instructors')->nullable();
            $table->json('Contact_Details_Confirmed')->nullable();
            $table->string('Q_Contract_Type')->nullable();
            $table->string('Q_Contract_Type_Other_Text')->nullable();
            $table->string('Q_Adult_Training_Only')->nullable();
            $table->json('Q_Expected_Delivery')->nullable();
            $table->tinyInteger('Q_Expected_Delivery_Empty')->default(0);

            $table->timestamp('Source_Created_At')->nullable();
            $table->timestamp('Source_Updated_At')->nullable();
            $table->timestamps();
        });

    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('Fact_Grant_Recipient_Renewal');
        Schema::dropIfExists('Fact_Instructor_Renewal');
        Schema::dropIfExists('Fact_Training_Provider_Renewal');
    }
};
